defmodule FlightDatastore do
  @moduledoc """
  google cloud module for getto/flight by peburrows/diplomat
  """

  alias FlightDatastore.Find
  alias FlightDatastore.Modify

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
  parse kinds to scopes
  then check permission to modify data
  and execute modify
  """
  def modify(data,scopes,credential) do
    if data |> Modify.check(scopes,credential) do
      data
      |> Modify.execute
      |> case do
        {:ok, response} ->
          keys = response |> Modify.inserted_keys
          data |> Modify.fill_keys(keys) |> Modify.log(scopes, credential)
          {:ok, %{keys: keys}}
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
