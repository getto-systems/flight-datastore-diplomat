defmodule FlightDatastore.Find do
  @moduledoc """
  Find entity utils
  """

  def to_key(namespace,kind,key) do
    Diplomat.Key.new(kind,key)
    |> case do
      key -> %{ key | namespace: namespace }
    end
  end
  def to_entity(properties,namespace,kind,key) do
    properties
    |> Diplomat.Entity.new(kind,key)
    |> case do
      entity ->
        key = %{ entity.key | namespace: namespace }
        %{ entity | key: key }
    end
  end

  @doc """
  Find by Diplomat
  return nil if error or not exists
  """
  def find_entity(namespace,kind,key) do
    to_key(namespace,kind,key)
    |> Diplomat.Key.get
    |> case do
      {:error, _} -> []
      result -> result
    end
    |> Enum.at(0)
  end

  @doc """
  Check for entity satisfy conditions

  ## Examples

      iex> FlightDatastore.Find.check(%{properties: %{"col" => %{value: "val"}}}, nil)
      %{properties: %{"col" => %{value: "val"}}}

      iex> FlightDatastore.Find.check(%{properties: %{"col" => %{value: "val"}}}, %{})
      %{properties: %{"col" => %{value: "val"}}}

      iex> FlightDatastore.Find.check(%{properties: %{"col" => %{value: "val"}}}, %{"col" => "val"})
      %{properties: %{"col" => %{value: "val"}}}

      iex> FlightDatastore.Find.check(%{properties: %{"col" => %{value: "val"}}}, %{"col" => "not matches"})
      nil

      iex> FlightDatastore.Find.check(%{properties: %{"col" => %{value: "val"}}}, %{"unknown col" => "not matches"})
      nil

      iex> FlightDatastore.Find.check(nil, %{"unknown col" => "not matches"})
      nil
  """
  def check(nil, _conditions), do: nil
  def check(entity, nil), do: entity
  def check(entity, conditions) do
    if entity |> properties_match?(conditions) do
      entity
    end
  end
  defp properties_match?(entity, conditions) do
    conditions
    |> Map.keys
    |> Enum.all?(fn col ->
      case entity.properties[col] do
        nil -> false
        data -> data.value == conditions[col]
      end
    end)
  end

  @doc """
  Convert entity to map

  ## Examples

      iex> FlightDatastore.Find.to_map(%{properties: %{"col" => %{value: "val"}}}, ["col"], %{})
      %{"col" => "val"}

      iex> FlightDatastore.Find.to_map(%{properties: %{"col" => %{value: "val"}}}, ["col","unknown"], %{})
      %{"col" => "val"}

      iex> FlightDatastore.Find.to_map(%{properties: %{"col" => %{value: "val"}}}, ["col","unknown"], %{"exclude": ["col"]})
      %{"col" => "val"}

      iex> FlightDatastore.Find.to_map(%{properties: %{"col" => %{value: "val"}}}, ["unknown"], %{})
      %{}

      iex> FlightDatastore.Find.to_map(%{properties: %{"col" => %{value: "val"}}}, [], %{})
      %{}

      iex> FlightDatastore.Find.to_map(%{properties: %{"col" => %{value: "val"}}}, nil, %{})
      %{}

      iex> FlightDatastore.Find.to_map(nil, nil, %{})
      nil
  """
  def to_map(entity, columns), do: to_map(entity, columns, %{})
  def to_map(nil, _columns, _scope), do: nil
  def to_map(_entity, nil, _scope), do: %{}
  def to_map(entity, columns, scope) do
    columns
    |> Enum.filter(fn col ->
      not ( (scope["exclude"] || []) |> Enum.member?(col) )
    end)
    |> Enum.reduce(%{}, fn col, acc ->
      if entity.properties[col] do
        acc |> Map.put(col, entity.properties[col].value)
      else
        acc
      end
    end)
  end
end
