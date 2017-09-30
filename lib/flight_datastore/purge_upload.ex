defmodule FlightDatastore.PurgeUpload do
  @moduledoc """
  Bulk insert entity utils
  """

  alias FlightDatastore.Scope
  alias FlightDatastore.Query
  alias FlightDatastore.BulkInsert

  @doc """
  Check permission to purge upload

  ## Examples

      iex> FlightDatastore.PurgeUpload.check([%{"kind" => "Upload", "action" => "delete", "dataKind" => "Item"}], %{"_" => %{"Upload" => %{"delete" => %{}}, "Item" => %{"delete" => %{}}}})
      true

      iex> FlightDatastore.PurgeUpload.check([%{"namespace" => "Namespace", "kind" => "Upload", "action" => "delete", "dataKind" => "Item"}], %{"Namespace" => %{"Upload" => %{"delete" => %{}}, "Item" => %{"delete" => %{}}}})
      true

      iex> FlightDatastore.PurgeUpload.check([%{"namespace" => "Namespace", "kind" => "Upload", "action" => "update", "dataKind" => "Item"}], %{"Namespace" => %{"Upload" => %{"update" => %{}}, "Item" => %{"delete" => %{}}}})
      false
  """
  def check(nil,_scopes), do: false
  def check([],_scopes), do: false
  def check(data,scopes) do
    data
    |> Enum.all?(fn info ->
      kind_scope = Scope.get(scopes,info["namespace"],info["kind"])
      dataKind_scope = Scope.get(scopes,info["namespace"],info["dataKind"])
      case {kind_scope,dataKind_scope} do
        {%{"delete" => _},%{"delete" => _}} -> true
        _ -> false
      end
    end)
  end

  def purge(info) do
    namespace = info["namespace"]
    kind = info["dataKind"]

    delete_all(namespace,kind,%{BulkInsert.file_column => info |> BulkInsert.file_signature})
    delete_all(namespace,info |> BulkInsert.error_kind,%{})
  end
  defp delete_all(namespace,kind,conditions) do
    keys = Query.keys(
      namespace,
      kind,
      conditions,
      500,
      0
    )
    if keys |> Enum.count > 0 do
      keys
      |> Enum.map(fn entity -> {:delete,entity.key} end)
      |> Diplomat.Entity.commit_request
      |> Diplomat.Client.commit

      delete_all(namespace,kind,conditions)
    end
  end
end
