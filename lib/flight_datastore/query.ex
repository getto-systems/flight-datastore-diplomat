defmodule FlightDatastore.Query do
  @moduledoc """
  Query execute utils
  """

  alias FlightDatastore.Scope

  def check(namespace,kind,conditions,limit,offset,scope) do
    case scope |> Scope.get(namespace,kind) do
      nil -> nil
      model_scope ->
        if is_valid_limit(limit,offset) and
          is_valid_conditions(conditions,model_scope)
        do
          model_scope
        end
    end
  end
  defp is_valid_limit(limit,offset) do
    is_integer(limit) and is_integer(offset)
  end
  defp is_valid_conditions(conditions,scope) do
    conditions
    |> Map.keys
    |> Enum.all?(fn key ->
      (scope["condition_cols"] || []) |> Enum.member?(key)
    end)
  end

  def execute(namespace,kind,conditions,limit,offset,scope) do
    select_all(kind)
    |> where(scope,conditions)
    |> limit(limit,offset)
    |> Diplomat.Query.new(conditions || %{})
    |> Diplomat.Query.execute(namespace)
  end

  def all_count(namespace,kind,conditions,scope) do
    if count(namespace,kind,conditions,scope,0) == 0 do
      0
    else
      max = find_max(namespace,kind,conditions,scope,scope["count_unit"] || 1000)
      find_count(namespace,kind,conditions,scope,(max/2), max)
    end
  end
  defp find_max(namespace,kind,conditions,scope,offset) do
    if offset > (scope["count_max"] || 100_000_000) do
      offset
    else
      if count(namespace,kind,conditions,scope,offset) > 0 do
        find_max(namespace,kind,conditions,scope,offset * 10)
      else
        offset
      end
    end
  end
  defp find_count(namespace,kind,conditions,scope,offset,max) do
    if max - offset < 0.5 do
      max |> round
    else
      if count(namespace,kind,conditions,scope,offset |> round) > 0 do
        find_count(namespace,kind,conditions,scope,offset + ((max - offset)/2),max)
      else
        find_count(namespace,kind,conditions,scope,offset - ((max - offset)/2),offset)
      end
    end
  end

  defp count(namespace,kind,conditions,scope,offset) do
    select_key(kind)
    |> where(scope,conditions)
    |> limit(1,offset)
    |> Diplomat.Query.new(conditions || %{})
    |> Diplomat.Query.execute(namespace)
    |> Enum.count
  end

  defp select_all(kind) do
    "select * from #{kind}"
  end
  defp select_key(kind) do
    "select __key__ from #{kind}"
  end
  defp where(query,scope,conditions) do
    clause = (scope["condition_cols"] || [])
              |> Enum.filter(fn col -> conditions |> Map.has_key?(col) end)
              |> Enum.map(fn key -> "#{key} = @#{key}" end)
    case clause do
      [] -> query
      _ -> "#{query} where #{clause |> Enum.join(" and ")}"
    end
  end
  defp limit(query,limit,offset) do
    "#{query} limit #{limit} offset #{offset}"
  end
end
