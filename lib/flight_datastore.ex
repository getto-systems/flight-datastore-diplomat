defmodule FlightDatastore do
  @moduledoc """
  google cloud module for getto/flight by peburrows/diplomat
  """

  alias FlightDatastore.Find

  @doc """
  find entity by key
  and check conditions (return nil if check fails)
  and convert to Map
  """
  def find(kind,key,conditions,columns) do
    Find.find_entity(kind,key)
    |> Find.check(conditions)
    |> Find.to_map(columns)
  end
end
