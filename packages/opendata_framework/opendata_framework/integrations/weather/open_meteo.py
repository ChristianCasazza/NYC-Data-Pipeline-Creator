# pipeline/utils/open_mateo_free_api.py

import logging
import time
from datetime import datetime, timedelta
from typing import Any

import requests
import polars as pl


##############################################################################
# Base Client with Retries, Backoff, and 429 Handling
##############################################################################

class OpenMeteoBaseClient:
    """
    A reusable base client for Open-Meteo with built-in:
      - Timeout
      - Retries (exponential backoff)
      - Rate-limit sleep on HTTP 429
      - Detailed logging
    """
    def __init__(
        self,
        base_url: str = "https://archive-api.open-meteo.com/v1/archive",
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
        logger: logging.Logger | None = None,
    ):
        self.base_url = base_url
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def _request(self, params: dict) -> dict:
        """
        GET with:
         - special 429 handling (sleep 60s and retry)
         - exponential backoff for other errors
         - raises RuntimeError after max_retries
        """
        attempt = 1
        while True:
            try:
                self.logger.debug(f"Attempt {attempt}: GET {self.base_url} with {params}")
                response = requests.get(
                    self.base_url,
                    params=params,
                    timeout=(self.connect_timeout, self.read_timeout),
                )

                # Rate-limit handling
                if response.status_code == 429:
                    self.logger.warning(
                        f"Rate limit hit (429). Sleeping 60s before retry "
                        f"(attempt {attempt}/{self.max_retries})"
                    )
                    time.sleep(60)
                    if attempt >= self.max_retries:
                        raise RuntimeError(f"Open-Meteo rate limit exceeded after {attempt} attempts.")
                    attempt += 1
                    continue

                if response.status_code == 200:
                    self.logger.debug("Request succeeded.")
                    return response.json()

                # Other HTTP errors
                raise requests.HTTPError(
                    f"Open-Meteo request failed (status={response.status_code}): {response.text}"
                )

            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
                self.logger.warning(
                    f"Request attempt {attempt} failed: {e}. "
                    f"{'Will retry.' if attempt < self.max_retries else 'No more retries.'}"
                )
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"Open-Meteo request failed after {attempt} attempts. Last error: {e}"
                    ) from e

                sleep_time = self.backoff_factor ** (attempt - 1)
                self.logger.info(f"Sleeping {sleep_time:.1f}s before retry...")
                time.sleep(sleep_time)
                attempt += 1


##############################################################################
# Daily Weather Client
##############################################################################

class OpenMateoDailyWeatherConfig:
    def __init__(
        self,
        start_date: str,
        end_date: str,
        latitude: float,
        longitude: float,
        timezone: str = "America/New_York",
        temperature_unit: str = "fahrenheit",
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.latitude = latitude
        self.longitude = longitude
        self.timezone = timezone
        self.temperature_unit = temperature_unit


class OpenMateoDailyWeatherClient(OpenMeteoBaseClient):
    def __init__(self, config: OpenMateoDailyWeatherConfig, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.config = config
        self.daily_vars = [
            "weathercode",
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "apparent_temperature_mean",
            "sunrise",
            "sunset",
            "precipitation_sum",
            "rain_sum",
            "snowfall_sum",
            "precipitation_hours",
        ]

    def fetch_daily_data(self, chunked: bool = False) -> pl.DataFrame:
        if not chunked:
            return self._fetch_single_range(self.config.start_date, self.config.end_date)
        self.logger.info("Fetching daily data in chunks.")
        return self._fetch_in_chunks()

    def _fetch_single_range(self, start_date: str, end_date: str) -> pl.DataFrame:
        params = {
            "latitude": self.config.latitude,
            "longitude": self.config.longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(self.daily_vars),
            "timezone": self.config.timezone,
            "temperature_unit": self.config.temperature_unit,
        }
        data = self._request(params)
        return self._process_response(data)

    def _fetch_in_chunks(self) -> pl.DataFrame:
        chunk_size = 30
        start_dt = datetime.strptime(self.config.start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(self.config.end_date, "%Y-%m-%d")
        all_frames = []
        current = start_dt
        while current <= end_dt:
            end_chunk = min(current + timedelta(days=chunk_size - 1), end_dt)
            self.logger.info(f"Fetching daily from {current:%Y-%m-%d} to {end_chunk:%Y-%m-%d}")
            df = self._fetch_single_range(current.strftime("%Y-%m-%d"), end_chunk.strftime("%Y-%m-%d"))
            if df.shape[0] > 0:
                all_frames.append(df)
            current = end_chunk + timedelta(days=1)
        return pl.concat(all_frames, how="vertical") if all_frames else pl.DataFrame()

    def _process_response(self, data: dict) -> pl.DataFrame:
        if "daily" not in data or "time" not in data["daily"]:
            self.logger.warning("No 'daily' data found in response.")
            return pl.DataFrame()
        daily = data["daily"]
        daily_data = {
            "date": [datetime.strptime(d, "%Y-%m-%d") for d in daily["time"]],
            "weather_code": daily["weathercode"],
            "temperature_max": daily["temperature_2m_max"],
            "temperature_min": daily["temperature_2m_min"],
            "temperature_mean": daily["temperature_2m_mean"],
            "apparent_temperature_max": daily["apparent_temperature_max"],
            "apparent_temperature_min": daily["apparent_temperature_min"],
            "apparent_temperature_mean": daily["apparent_temperature_mean"],
            "sunrise": daily["sunrise"],
            "sunset": daily["sunset"],
            "precipitation_sum": daily["precipitation_sum"],
            "rain_sum": daily["rain_sum"],
            "snowfall_sum": daily["snowfall_sum"],
            "precipitation_hours": daily["precipitation_hours"],
        }
        df = pl.DataFrame(daily_data)
        self.logger.info(f"Fetched daily data: {df.shape[0]} rows.")
        return df


##############################################################################
# Hourly Weather Client
##############################################################################

class OpenMateoHourlyWeatherConfig:
    def __init__(
        self,
        start_date: str,
        end_date: str,
        latitude: float,
        longitude: float,
        timezone: str = "America/New_York",
        temperature_unit: str = "fahrenheit",
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.latitude = latitude
        self.longitude = longitude
        self.timezone = timezone
        self.temperature_unit = temperature_unit


class OpenMateoHourlyWeatherClient(OpenMeteoBaseClient):
    def __init__(self, config: OpenMateoHourlyWeatherConfig, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.config = config
        self.hourly_vars = ["temperature_2m", "precipitation", "rain", "weathercode"]

    def fetch_hourly_data(self, chunked: bool = False) -> pl.DataFrame:
        if not chunked:
            return self._fetch_single_range(self.config.start_date, self.config.end_date)
        self.logger.info("Fetching hourly data in chunks.")
        return self._fetch_in_chunks()

    def _fetch_single_range(self, start_date: str, end_date: str) -> pl.DataFrame:
        params = {
            "latitude": self.config.latitude,
            "longitude": self.config.longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(self.hourly_vars),
            "timezone": self.config.timezone,
            "temperature_unit": self.config.temperature_unit,
        }
        data = self._request(params)
        return self._process_response(data)

    def _fetch_in_chunks(self) -> pl.DataFrame:
        chunk_size = 7
        start_dt = datetime.strptime(self.config.start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(self.config.end_date, "%Y-%m-%d")
        all_frames = []
        current = start_dt
        while current <= end_dt:
            end_chunk = min(current + timedelta(days=chunk_size - 1), end_dt)
            self.logger.info(f"Fetching hourly from {current:%Y-%m-%d} to {end_chunk:%Y-%m-%d}")
            df = self._fetch_single_range(current.strftime("%Y-%m-%d"), end_chunk.strftime("%Y-%m-%d"))
            if df.shape[0] > 0:
                all_frames.append(df)
            current = end_chunk + timedelta(days=1)
        return pl.concat(all_frames, how="vertical") if all_frames else pl.DataFrame()

    def _process_response(self, data: dict) -> pl.DataFrame:
        if "hourly" not in data or "time" not in data["hourly"]:
            self.logger.warning("No 'hourly' data found in response.")
            return pl.DataFrame()
        hourly = data["hourly"]
        hourly_data = {
            "date": [datetime.strptime(dt, "%Y-%m-%dT%H:%M") for dt in hourly["time"]],
            "temperature_2m": hourly["temperature_2m"],
            "precipitation": hourly["precipitation"],
            "rain": hourly["rain"],
            "weather_code": hourly["weathercode"],
        }
        df = pl.DataFrame(hourly_data)
        self.logger.info(f"Fetched hourly data: {df.shape[0]} rows.")
        return df
