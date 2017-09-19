defmodule FlightDatastore do
  @moduledoc """
  google cloud module for getto/flight by peburrows/diplomat
  """

  alias FlightDatastore.Find
  alias FlightDatastore.Modify
  alias FlightDatastore.BulkInsert

  @doc """
  find entity by key
  and check conditions (return nil if check fails)
  and convert to Map
  """
  def find(kind,key,conditions,columns,scope) do
    Find.find_entity(kind,key)
    |> Find.check(conditions)
    |> Find.to_map(columns,scope)
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
          status |> to_error
      end
    else
      {:error, :not_allowed}
    end
  end

  @doc """
  parse file as data per line
  then insert data
  """
  def bulk_insert(data,kind,scope) do
    [%{
      "kind" => kind,
      "action" => "insert",
      "properties" => data |> BulkInsert.filter(scope["cols"]),
    }]
    |> Modify.execute
    |> case do
      {:ok, response} ->
        {:ok, response |> Modify.inserted_keys}
      {:error, status} ->
        {:error, type, message} = status |> to_error
        {:error, "#{type}: #{message}"}
    end
  end

  defp to_error(status) do
    case status.code do
      5 -> {:error, :not_found,   status.message}
      6 -> {:error, :conflict,    status.message}
      _ -> {:error, :bad_request, status.message}
    end
  end
end
