defmodule FlightDatastore.BulkInsert do
  @moduledoc """
  Bulk insert entity utils
  """

  alias FlightDatastore.Modify

  @error_kind "_Flight_BulkInsertError"
  @key_column "_bulk_insert_key"
  @file_column "_bulk_insert_file"

  def insert_data(info,scope) do
    case open_output(info.dest,info.data["name"]) do
      {:ok,out} ->
        case open_input(info.src,info.data["name"]) do
          {:ok, file} ->
            result = file
            |> IO.stream(:line)
            |> Enum.reduce(true, fn line, acc ->
              data = line |> Poison.decode! |> fill(scope,info.data)
              case data |> insert(info) do
                {:ok, _response} ->
                  out |> IO.puts(data |> Poison.encode!)
                  acc and true
                {:error, status} ->
                  insert_error(data,status,info.data)
                  false
              end
            end)

            file |> File.close
            out  |> File.close
            {:ok, result}

          error -> error
        end
      error -> error
    end
  end

  defp open_input(src,file) do
    [src,file] |> Path.join |> open_file
  end
  defp open_output(dest,file) do
    path = [dest,file] |> Path.join
    case File.mkdir_p(path |> Path.dirname) do
      {:error, message} -> {:error, "failed mkdir_p: #{message} [#{path}]"}
      _ -> path |> open_file([:write])
    end
  end
  defp open_file(path,mode \\ []) do
    case path |> File.open([:utf8 | mode]) do
      {:error, message} -> {:error, "failed open file: #{message} [#{path}]"}
      result -> result
    end
  end


  defp fill(data, scope, info) do
    # TODO fill data
    data
    |> Map.put(@key_column, generate_key(data, scope["keys"], info))
    |> Map.put(@file_column, info |> file_signature)
  end

  @doc """
  Generate key

  ## Examples

      iex> FlightDatastore.BulkInsert.generate_key(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, ["column1","column2"], %{"namespace" => "Namespace", "kind" => "File", "name" => "file.txt"})
      "Namespace:File:file.txt:value1:value2"

      iex> FlightDatastore.BulkInsert.generate_key(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, ["column_unknown"], %{"namespace" => "Namespace", "kind" => "File", "name" => "file.txt"})
      "Namespace:File:file.txt:"

      iex> FlightDatastore.BulkInsert.generate_key(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, [], %{"kind" => "File", "name" => "file.txt"})
      ":File:file.txt"
  """
  def generate_key(data, keys, info) do
    [info |> file_signature | keys |> Enum.map(fn key -> data[key] end)] |> Enum.join(":")
  end

  defp insert(data,info) do
    [%{
      "action" => info.action,
      "namespace" => info.data["namespace"],
      "kind" => info.data[info.data_kind],
      "key" => data[@key_column],
      "properties" => data,
    }]
    |> Modify.execute
  end

  defp insert_error(data,status,info) do
    [%{
      "kind" => info |> error_kind,
      "action" => "insert",
      "properties" => %{
        "data" => data,
        "message" => status.message,
      },
    }]
    |> Modify.execute
  end


  def save_result(info,result,message) do
    [%{
      "namespace" => info.data["namespace"],
      "kind" => info.data["kind"],
      "key" => info.data["name"],
      "action" => "update",
      "properties" => %{
        "bulk_insert_#{info.data_kind}" => %{
          result: result,
          message: message,
          error: info.data |> error_kind,
        },
      },
    }]
    |> Modify.execute

    info |> Modify.log(info.credential)

    info.data
  end

  def file_column do
    @file_column
  end
  def error_kind(info) do
    "#{@error_kind}:#{info |> file_signature}"
  end
  def file_signature(info) do
    "#{info["namespace"]}:#{info["kind"]}:#{info["name"]}"
  end
end
