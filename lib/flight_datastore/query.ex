defmodule FlightDatastore.Query do
  @moduledoc """
  Query execute utils
  """

  alias FlightDatastore.Scope

  def check(namespace,kind,conditions,order_column,limit,offset,scope) do
    case scope |> Scope.get(namespace,kind) do
      nil -> nil
      model_scope ->
        if is_valid_limit(limit,offset) and
          is_valid_order(order_column,model_scope) and
          is_valid_conditions(conditions,model_scope)
        do
          model_scope
        end
    end
  end
  defp is_valid_limit(limit,offset) do
    is_integer(limit) and is_integer(offset)
  end
  defp is_valid_order(order_column,scope) do
    (scope["order_cols"] || []) |> Enum.member?(order_column)
  end
  defp is_valid_conditions(conditions,scope) do
    conditions
    |> Map.keys
    |> Enum.all?(fn key ->
      (scope["condition_cols"] || []) |> Enum.member?(key)
    end)
  end

  def execute(namespace,kind,conditions,order_column,order,limit,offset) do
    select_all(kind)
    |> where(conditions)
    |> order_by(order_column,order)
    |> limit(limit,offset)
    |> Diplomat.Query.new(conditions || %{})
    |> Diplomat.Query.execute(namespace)
  end

  def all_count(namespace,kind,conditions,scope) do
    if count(namespace,kind,conditions,0) == 0 do
      0
    else
      count_unit = scope["count_unit"] || 1000
      count_max = scope["count_max"] || 100_000_000
      max = find_max(namespace,kind,conditions,count_max,count_unit)
      find_count(namespace,kind,conditions,(max/2), max)
    end
  end
  defp find_max(namespace,kind,conditions,max,offset) do
    if offset > max do
      offset
    else
      if count(namespace,kind,conditions,offset) > 0 do
        find_max(namespace,kind,conditions,max,offset * 10)
      else
        offset
      end
    end
  end
  defp find_count(namespace,kind,conditions,offset,max) do
    if max - offset < 0.5 do
      max |> round
    else
      if count(namespace,kind,conditions,offset |> round) > 0 do
        find_count(namespace,kind,conditions,offset + ((max - offset)/2),max)
      else
        find_count(namespace,kind,conditions,offset - ((max - offset)/2),offset)
      end
    end
  end

  defp count(namespace,kind,conditions,offset) do
    select_key(kind)
    |> where(conditions)
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
  defp where(query,conditions) do
    clause = (conditions || %{})
             |> Map.keys
             |> Enum.map(fn key -> "#{key} = @#{key}" end)
    case clause do
      [] -> query
      _ -> "#{query} where #{clause |> Enum.join(" and ")}"
    end
  end
  defp order_by(query,order_column,order) do
    unless order_column do
      query
    else
      "#{query} order by #{order_column} #{if order, do: "asc", else: "desc"}"
    end
  end
  defp limit(query,limit,offset) do
    "#{query} limit #{limit} offset #{offset}"
  end
end
