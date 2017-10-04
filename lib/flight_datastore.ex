defmodule FlightDatastore do
  @moduledoc """
  google cloud module for getto/flight by peburrows/diplomat
  """

  alias FlightDatastore.Scope
  alias FlightDatastore.Find
  alias FlightDatastore.Query
  alias FlightDatastore.Modify
  alias FlightDatastore.BulkInsert
  alias FlightDatastore.PurgeUpload

  @doc """
  find entity by key
  and check conditions (return nil if check fails)
  and convert to Map
  """
  def find(info) do
    case info.scope |> Scope.get(info.namespace,info.kind) do
      nil -> {:error, :not_allowed}
      model_scope ->
        {:ok,
          Find.find_entity(info.namespace,info.kind,info.key)
          |> Find.check(info.conditions)
          |> Find.to_map(info.columns,model_scope)
        }
    end
  end

  @doc """
  execute query
  and convert to Map
  """
  def query(info) do
    case Query.check(info) do
      nil -> {:error, :not_allowed}
      model_scope ->
        case Query.execute(info) do
          {:error, status} ->
            case status.code do
              9 -> {:error, :not_allowed, status.message}
              _ -> {:error, :bad_request, status.message}
            end
          result ->
              {:ok, %{
                result: result |> Enum.map(fn entity ->
                  entity |> Find.to_map(info.columns,model_scope)
                end),
                count: model_scope |> Query.all_count(info),
              }}
        end
    end
  end

  @doc """
  check permission to modify data
  and execute modify
  """
  def modify(info) do
    if info.data |> Modify.check(info.scope,info.credential) do
      info.data
      |> Modify.execute
      |> case do
        {:ok, response} ->
          keys = response |> Modify.inserted_keys
          filled = info.data |> Modify.fill_keys(keys)
          filled |> Modify.log(info.scope, info.credential)
          {:ok, %{keys: keys, data: filled}}
        {:error, status} ->
          case status.code do
            5 -> {:error, :not_found,   status.message}
            6 -> {:error, :conflict,    status.message}
            _ -> {:error, :bad_request, status.message}
          end
      end
    else
      {:error, :not_allowed}
    end
  end

  @doc """
  Generate key and Fill data
  then insert data and output data
  """
  def bulk_insert(info) do
    case info.scope |> Scope.get(info.data["namespace"],info.data[info.data_kind]) do
      nil ->
        info |> BulkInsert.save_result(false,"not allowed")
        {:error, :not_allowed}
      model_scope ->
        case info |> BulkInsert.insert_data(model_scope) do
          {:ok, result} ->
            {:ok,
              info |> BulkInsert.save_result(result,"ok")
            }
          error -> error
        end
    end
  end

  @doc """
  Purge upload data and bulk insert data
  """
  def purge_upload(info) do
    request =
      info.data
      |> Enum.map(fn data ->
        Find.find_entity(data["namespace"],data["kind"],data["key"])
        |> Find.to_map(Enum.concat(["name"],info.data_kinds))
        |> Map.put("namespace", data["namespace"])
        |> Map.put("kind", data["kind"])
        |> Map.put("key", data["key"])
        |> Map.put("action", "delete")
      end)

    if request |> PurgeUpload.check(info.data_kinds,info.scope) do
      request
      |> Modify.execute
      |> case do
        {:ok, _} ->
          request |> Modify.log(info.scope, info.credential)
          request
          |> Enum.each(fn data ->
            data |> PurgeUpload.purge(info.data_kinds)
          end)
          {:ok, request}
        {:error, status} ->
          case status.code do
            5 -> {:error, :not_found,   status.message}
            6 -> {:error, :conflict,    status.message}
            _ -> {:error, :bad_request, status.message}
          end
      end
    else
      {:error, :not_allowed}
    end
  end
end
