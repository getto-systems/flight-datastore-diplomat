defmodule FlightDatastore do
  @moduledoc """
  google cloud module for getto/flight by peburrows/diplomat
  """

  alias FlightDatastore.Scope
  alias FlightDatastore.Find
  alias FlightDatastore.Query
  alias FlightDatastore.Modify
  alias FlightDatastore.BulkInsert

  @doc """
  find entity by key
  and check conditions (return nil if check fails)
  and convert to Map
  """
  def find(namespace,kind,key,conditions,columns,scope) do
    case scope |> Scope.get(namespace,kind) do
      nil -> {:error, :not_allowed}
      model_scope ->
        {:ok,
          Find.find_entity(namespace,kind,key)
          |> Find.check(conditions)
          |> Find.to_map(columns,model_scope)
        }
    end
  end

  @doc """
  execute query
  and convert to Map
  """
  def query(namespace,kind,conditions,columns,order_column,order,limit,offset,scope) do
    case Query.check(namespace,kind,conditions,order_column,limit,offset,scope) do
      nil -> {:error, :not_allowed}
      model_scope ->
        case Query.execute(namespace,kind,conditions,order_column,order,limit,offset) do
          {:error, message} -> {:error, :execute_failed, message}
          result ->
              {:ok, %{
                result: result |> Enum.map(fn entity ->
                  entity |> Find.to_map(columns,model_scope)
                end),
                count: Query.all_count(namespace,kind,conditions,model_scope),
              }}
        end
    end
  end

  @doc """
  check permission to modify data
  and execute modify
  """
  def modify(data,scopes,credential) do
    if data |> Modify.check(scopes,credential) do
      data
      |> Modify.execute
      |> case do
        {:ok, response} ->
          keys = response |> Modify.inserted_keys
          filled = data |> Modify.fill_keys(keys)
          filled |> Modify.log(scopes, credential)
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
  def bulk_insert(info,src,dest,scope,credential) do
    case scope |> Scope.get(info["namespace"],info["dataKind"]) do
      nil ->
        info |> BulkInsert.save_result(false,"not allowed",credential)
        {:error, :not_allowed}
      model_scope ->
        case info |> BulkInsert.insert_data(src,dest,model_scope) do
          {:ok, result} ->
            {:ok,
              info |> BulkInsert.save_result(result,"ok",credential)
            }
          error -> error
        end
    end
  end
end
