defmodule FlightDatastore.Query do
  @moduledoc """
  Query execute utils
  """

  alias FlightDatastore.Scope

  def check(info) do
    case info.scope |> Scope.get(info.namespace,info.kind) do
      nil -> nil
      model_scope ->
        if is_valid_limit(info.limit,info.offset) and
          is_valid_order(info.order_column,model_scope) and
          is_valid_conditions(info.conditions,model_scope)
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

  def execute(info) do
    select_all(info.kind)
    |> where(info.conditions)
    |> order_by(info.order_column,info.order)
    |> limit(info.limit,info.offset)
    |> Diplomat.Query.new(info.conditions || %{})
    |> Diplomat.Query.execute(info.namespace)
  end

  def keys(info) do
    select_key(info.kind)
    |> where(info.conditions)
    |> limit(info.limit,info.offset)
    |> Diplomat.Query.new(info.conditions || %{})
    |> Diplomat.Query.execute(info.namespace)
  end

  def all_count(scope,info) do
    if count(info,0) == 0 do
      0
    else
      count_unit = scope["count_unit"] || 1000
      count_max = scope["count_max"] || 100_000_000
      max = find_max(info,count_max,count_unit)
      find_count(info,(max/2), max)
    end
  end
  defp find_max(info,max,offset) do
    if offset > max do
      offset
    else
      if count(info,offset) > 0 do
        find_max(info,max,offset * 10)
      else
        offset
      end
    end
  end
  defp find_count(info,offset,max) do
    if max - offset < 0.5 do
      max |> round
    else
      if count(info,offset |> round) > 0 do
        find_count(info,offset + ((max - offset)/2),max)
      else
        find_count(info,offset - ((max - offset)/2),offset)
      end
    end
  end

  defp count(info,offset) do
    select_key(info.kind)
    |> where(info.conditions)
    |> limit(1,offset)
    |> Diplomat.Query.new(info.conditions || %{})
    |> Diplomat.Query.execute(info.namespace)
    |> Enum.count
  end

  defp select_all(kind) do
    "select * from `#{kind}`"
  end
  defp select_key(kind) do
    "select __key__ from `#{kind}`"
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
