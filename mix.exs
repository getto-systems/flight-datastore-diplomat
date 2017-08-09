defmodule FlightDatastore.Mixfile do
  use Mix.Project

  def project do
    [
      app: :flight_datastore,
      version: "0.1.0",
      elixir: "~> 1.5",
      start_permanent: Mix.env == :prod,
      deps: deps(),
      escript: escript(),
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :goth]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:diplomat, "~> 0.8.2"},
      {:goth, "~> 0.5.0"},
    ]
  end

  defp escript do
    [main_module: FlightDatastore.CLI, path: "_build/escript/flight_datastore"]
  end
end
