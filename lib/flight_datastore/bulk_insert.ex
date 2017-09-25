defmodule FlightDatastore.BulkInsert do
  @moduledoc """
  Bulk insert entity utils
  """

  alias FlightDatastore.Modify

  @error_kind "_Flight_BulkInsertError"
  @key_column "_bulk_insert_key"
  @file_column "_bulk_insert_file"

  def insert_data(info,src,dest,scope) do
    case open_output(dest,info["name"]) do
      {:ok,out} ->
        case open_input(src,info["name"]) do
          {:ok, file} ->
            result = file
            |> IO.stream(:line)
            |> Enum.reduce(true, fn line, acc ->
              data = line |> Poison.decode! |> fill(scope,info)
              case data |> insert(info) do
                {:ok, _response} ->
                  out |> IO.puts(data |> Poison.encode!)
                  acc and true
                {:error, status} ->
                  insert_error(data,status,info)
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

  defp insert(data,info) do
    [%{
      "action" => "insert",
      "namespace" => info["namespace"],
      "kind" => info["dataKind"],
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


  def save_result(info,result,message,credential) do
    info = info |> Map.put(:bulk_insert, %{
      result: result,
      message: message,
      error: info |> error_kind,
    })
    [%{
      "namespace" => info["namespace"],
      "kind" => info["kind"],
      "key" => info["name"],
      "action" => "update",
      "properties" => info,
    }]
    |> Modify.execute

    info |> Modify.log(credential)

    info
  end

  defp error_kind(info) do
    "#{@error_kind}:#{info |> file_signature}"
  end

  defp file_signature(info) do
    "#{info["namespace"]}:#{info["kind"]}:#{info["name"]}"
  end
end
