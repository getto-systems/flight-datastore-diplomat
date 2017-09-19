defmodule FlightDatastore.BulkInsert do
  @moduledoc """
  Bulk insert entity utils
  """

  @doc """
  Filter columns

  ## Examples

      iex> FlightDatastore.BulkInsert.filter(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, ["column1","column2"])
      %{"column1" => "value1", "column2" => "value2"}

      iex> FlightDatastore.BulkInsert.filter(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, ["column4"])
      %{"column4" => nil}

      iex> FlightDatastore.BulkInsert.filter(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, [])
      %{}
  """
  def filter(data,cols) do
    cols
    |> Enum.reduce(%{}, fn col, acc ->
      acc |> Map.put(col, data[col])
    end)
  end
end
