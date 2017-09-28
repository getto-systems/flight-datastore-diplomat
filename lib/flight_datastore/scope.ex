defmodule FlightDatastore.Scope do
  @moduledoc """
  Scope entity utils
  """

  @doc """
  Get scope by namespace and kind

  ## Examples

      iex> FlightDatastore.Scope.get(%{"Namespace" => %{"Kind" => %{scope: :scope}}}, "Namespace", "Kind")
      %{scope: :scope}

      iex> FlightDatastore.Scope.get(%{"_" => %{"Kind" => %{scope: :scope}}}, nil, "Kind")
      %{scope: :scope}

      iex> FlightDatastore.Scope.get(%{"Namespace" => %{"Kind" => %{scope: :scope}}}, "OtherNamespace", "Kind")
      nil

      iex> FlightDatastore.Scope.get(%{"Namespace" => %{"Kind" => %{scope: :scope}}}, "Namespace", "OtherKind")
      nil

      iex> FlightDatastore.Scope.get(%{"Namespace" => %{"Kind" => %{scope: :scope}}}, "OtherNamespace", "OtherKind")
      nil

      iex> FlightDatastore.Scope.get(%{"Namespace" => %{"Kind" => %{scope: :scope}, nil => %{scope: :other}}}, "OtherNamespace", "OtherKind")
      nil

      iex> FlightDatastore.Scope.get(%{"~Namespace_" => %{"Kind" => %{scope: :scope}}}, "Namespace_Suffix", "Kind")
      %{scope: :scope}

      iex> FlightDatastore.Scope.get(%{"~Namespace_" => %{"~Kind_" => %{scope: :scope}}}, "Namespace_2017_Suffix", "Kind_2017_Suffix")
      %{scope: :scope}

      iex> FlightDatastore.Scope.get(%{"~Namespace_" => %{"~Kind_" => %{scope: :scope}}}, "Namespace_ 'not allowed char exists'", "Kind_ 'not allowed char exists'")
      nil
  """
  def get(scope,namespace,kind) do
    if namespace_scope = scope_match(scope,namespace || "_") do
      scope_match(namespace_scope,kind)
    end
  end
  defp scope_match(scope,match) do
    scope
    |> Map.keys
    |> Enum.find_index(fn key ->
      case key do
        "~" <> prefix ->
          Regex.match?(~r{^[0-9a-zA-Z_]+$}, match) and
          (match |> String.starts_with?(prefix))

        ^match -> true
        _      -> false
      end
    end)
    |> case do
      nil -> nil
      idx -> scope |> Map.values |> Enum.at(idx)
    end
  end
end
