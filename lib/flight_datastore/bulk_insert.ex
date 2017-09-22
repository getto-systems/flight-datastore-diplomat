defmodule FlightDatastore.BulkInsert do
  @moduledoc """
  Bulk insert entity utils
  """

  alias FlightDatastore.Modify

  @error_kind "_Flight_BulkInsertError"
  @key_column "_bulk_insert_key"
  @file_column "_bulk_insert_file"

  @doc """
  Fill key, relational data
  """
  def fill(data, keys, _fill, info) do
    # TODO fill data
    data
    |> Map.put(@key_column, generate_key(data, keys, info))
    |> Map.put(@file_column, info |> file_signature)
  end

  @doc """
  Generate key

  ## Examples

      iex> FlightDatastore.BulkInsert.generate_key(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, ["column1","column2"], %{"kind" => "File", "name" => "file.txt"})
      "File:file.txt:value1:value2"

      iex> FlightDatastore.BulkInsert.generate_key(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, ["column_unknown"], %{"kind" => "File", "name" => "file.txt"})
      "File:file.txt:"

      iex> FlightDatastore.BulkInsert.generate_key(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, [], %{"kind" => "File", "name" => "file.txt"})
      "File:file.txt"
  """
  def generate_key(data, keys, info) do
    [info |> file_signature | keys |> Enum.map(fn key -> data[key] end)] |> Enum.join(":")
  end

  def insert(data,kind,info) do
    [%{
      "kind" => kind,
      "action" => "insert",
      "key" => data[@key_column],
      "properties" => data,
    }]
    |> Modify.execute
    |> case do
      {:ok, _response} -> :ok
      {:error, status} ->
        [%{
          "kind" => info |> error_kind,
          "action" => "insert",
          "properties" => %{
            "data" => data,
            "message" => status.message,
          },
        }]
        |> Modify.execute
        :error
    end
  end

  def error_kind(info) do
    "#{@error_kind}:#{info |> file_signature}"
  end

  defp file_signature(info) do
    ["kind","name"]
    |> Enum.map(fn key -> info[key] end)
    |> Enum.join(":")
  end
end
