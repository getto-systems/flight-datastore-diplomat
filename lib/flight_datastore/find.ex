defmodule FlightDatastore.Find do
  def find_entity(kind,key) do
    Diplomat.Key.new(kind,key)
    |> Diplomat.Key.get
    |> case do
      {:error, _} -> []
      result -> result
    end
    |> Enum.at(0)
  end

  def check(entity, conditions) do
    if entity do
      case conditions do
        nil -> entity
        conditions ->
          if entity |> properties_match?(conditions) do
            entity
          else
            nil
          end
      end
    end
  end
  def properties_match?(entity, conditions) do
    conditions
    |> Map.keys
    |> Enum.all?(fn col ->
      case entity.properties[col] do
        nil -> false
        data -> data.value == conditions[col]
      end
    end)
  end

  def to_map(entity, columns) do
    if entity do
      case columns do
        nil -> %{}
        columns ->
          columns |> Enum.reduce(%{}, fn col, acc ->
            if entity.properties[col] do
              acc |> Map.put(col, entity.properties[col].value)
            else
              acc
            end
          end)
      end
    end
  end
end
