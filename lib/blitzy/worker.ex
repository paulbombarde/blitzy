defmodule Blitzy.Worker do
  use Timex
  require Logger

  def run(n, url, timeout \\ 5000) do
    async_query = fn -> query(url) end
    1..n
    |> Enum.map(fn _ -> Task.async(async_query) end)
    |> Enum.map(&Task.await(&1, timeout)) 
  end

  def query(url) do
    {time, result} = Timex.Duration.measure(HTTPoison, :get, [url])
    handle_response(Timex.Duration.to_milliseconds(time), result)
  end

  defp handle_response(duration, {:ok, %HTTPoison.Response{status_code: status_code}}) when 200 <= status_code and status_code <= 304 do
    Logger.info "worker #{node()}-#{inspect self()} completed in #{duration} msecs"
    {:ok, duration}
  end

  defp handle_response(_duration, {:error, %HTTPoison.Error{reason: reason}}) do
    Logger.error "worker #{node()}-#{inspect self()} error due to #{inspect reason}"
    {:error, reason}
  end

  defp handle_response(_duration, _) do
    Logger.error "worker #{node()}-#{inspect self()} unknown error"
    {:error, :unknown}
  end

  def handle_results(results) do
    success_times = 
      results
      |> Enum.filter(fn r ->
        case r do
          {:ok, _t} -> true
          _ -> false
        end
      end)
      |> Enum.map(fn {:ok, t} -> t end)

    total_queries = Enum.count(results)
    total_successes = Enum.count(success_times)
    total_failures = total_queries - total_successes

    total_time = Enum.sum(success_times)
    average_time = total_time/total_successes
    max_time = Enum.max(success_times)
    min_time = Enum.min(success_times)

    IO.puts """
    Total queries       : #{total_queries}
    Total successes     : #{total_successes}
    Total failures      : #{total_failures}

    Average time (msec) : #{average_time}
    Max time (msec)     : #{max_time}
    Min time (msec)     : #{min_time}
    """
  end
end
