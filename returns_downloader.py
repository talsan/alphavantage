import requests
from utils import s3_helpers, athena_helpers
from datetime import datetime
import config
import pandas as pd
import argparse
import os
import time
from io import StringIO
import boto3
import logging
from operator import itemgetter

log = logging.getLogger(__name__)

'''
Overview: 
    For a given date range and ticker, this script: 
        1) Determines which tickers need to be downloaded (with file update/overwrite optionality)
            via: build_ticker_download_list(rundate, overwrite=False)
        2) Downloads the json object containing prices, computes returns, formats it, and uploads it to aws s3
            via ReturnsProcessor object

Intended Usage:
    1) As a script, running on a scheduler or manually.   
        Example:
            c:/../av>python returns_downloader.py IWV daily 2020-05-31 --overwrite

Input Parameters (command line arguments): 
    ticker -- ETF ticker you wish to download
    rundate -- end-of-month date at which full history of returns are being refreshed
    period -- Daily (for daily returns) or Monthly (for monthly returns)
    overwrite -- Overwrite holdings that have already been downloaded to aws s3

Output: 
    aws s3 files:
        path = see config.av

'''

'''
need to remove asterisk in ticker
gzip outputs
strategy for naming s3 pipelines, athena tables, etc.
    source not necessary, run date should be after full
period before rundate
timestamp for download
handle upper/lower case tickers (dont assume vendor will be consistent)
remove extension in s3 list
add av key to log
does it need to be count + 1?
sand for s3
'''

# -----------------------------------------------------------------------------------
# misc configurations
# -----------------------------------------------------------------------------------
aws_session = boto3.Session(aws_access_key_id=config.Access.AWS_KEY,
                            aws_secret_access_key=config.Access.AWS_SECRET)


# ReturnsProcessor manages the data-pipeline from alphavantage.com to aws s3
class ReturnsProcessor():
    # class variables, keeping track of states/data across class instances
    api_key = config.Access.AV_KEY_LIST[0]  # initialize the api key to the first key in the list
    key_usage_count = 0  # counts the number of consecutive times this api key has been used (to handle av throttle)
    api_keys_exausted = False  # when true the main() will exit
    invalid_tickers = []

    def __init__(self, ticker, period, rundate):
        # ticker-level information
        self.ticker = ticker
        self.period = period
        self.rundate = rundate
        self.download_timestamp = ''
        self.ticker_time_series = {}
        self.prices = {}
        self.ret_df = pd.DataFrame()

        # s3 info
        self.s3_client = aws_session.client('s3', region_name=config.AlphaVantage.S3_REGION_NAME)
        self.s3_response = ''  # returned from aws after s3-upload

        # TODO: hardcoded strings for daily/monthly - ok?
        if self.period.lower() == 'daily':
            self.fstring = 'TIME_SERIES_DAILY_ADJUSTED'
            self.jsonstring = 'Time Series (Daily)'
        elif self.period.lower() == 'monthly':
            self.fstring = 'TIME_SERIES_MONTHLY_ADJUSTED'
            self.jsonstring = 'Monthly Adjusted Time Series'
        else:
            raise ('only "daily" or "monthly" are acceptable inputs into period')

        log.info(f'ReturnsProcessor for {self.ticker} successfully initialized')

    def request_prices(self):
        # get the time stamp
        self.download_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # get current system time

        # loop until an api_key that can be used 'today' is found
        num_tries = 0
        while num_tries < len(config.Access.AV_KEY_LIST):
            request = dict(url='https://www.alphavantage.co/query',
                           params={'function': f'{self.fstring}',
                                   'symbol': self.ticker.replace('*', ''),
                                   'outputsize': 'full',
                                   'apikey': ReturnsProcessor.api_key})

            response = requests.get(**request)
            self.ticker_time_series = response.json()

            key_index = config.Access.AV_KEY_LIST.index(ReturnsProcessor.api_key)

            # if no api limit warning
            if self.ticker_time_series.get('Note') is None:
                ReturnsProcessor.key_usage_count = (ReturnsProcessor.key_usage_count + 1)
                sleep_between_requests(ReturnsProcessor.key_usage_count)
                log.info(f'successful api key index {key_index + 1} for iteration {ReturnsProcessor.key_usage_count}')
                return self

            # if note that we've used up all api keys for the day
            elif self.ticker_time_series['Note'][:9].lower() == 'thank you':
                log.info(f'api key index {key_index + 1} at capacity after {ReturnsProcessor.key_usage_count} iterations; '
                         f'moving to api key index {key_index + 2}')

                new_key_index = (key_index + 1) % len(config.Access.AV_KEY_LIST) ### is this needed since the while loop is num_tries < len(av_key_list)?
                ReturnsProcessor.api_key = config.Access.AV_KEY_LIST[new_key_index]
                ReturnsProcessor.key_usage_count = 1
                num_tries += 1

        ReturnsProcessor.api_keys_exausted = True
        return self

    def validate_price_object(self):

        try:
            y = self.ticker_time_series['Meta Data']

            # assert that the data has been refreshed recently
            last_refreshed = self.ticker_time_series['Meta Data'].get('3. Last Refreshed')
            if last_refreshed is not None:
                if self.rundate > last_refreshed:
                    log.warning(f'alpha vantage pricing is delayed; last refreshed on {last_refreshed}')
            else:
                log.error('error: alpha vantage is missing an expected item ("3. Last Refreshed") in their Meta Data')

            return True

        except KeyError as e:
            log.error(f'error: improper response object from alpha vantage for ticker_proxy = {self.ticker}')
            ReturnsProcessor.invalid_tickers.append(self.ticker)
            return False

    def load_prices_into_df(self):
        # create a self-contained dictionary of values, so we can easily form the dataframe below
        self.prices = self.ticker_time_series[f'{self.jsonstring}']
        for ymd in self.prices.keys():
            self.prices[ymd]['date'] = ymd
            self.prices[ymd]['ticker'] = self.ticker.upper()  # always upper case tickers in dataset
            self.prices[ymd]['download_timestamp'] = self.download_timestamp

        # form a list of dictionaries sorted by date and load into dataframe
        ret = sorted(list(self.prices.values()), key=itemgetter('date'))
        self.ret_df = pd.DataFrame(ret)

        # re-format names of columns - only use alphabetic characters and underscore
        new_columns = self.ret_df.columns \
            .str.lower() \
            .str.replace('[^a-z_\s]', '') \
            .str.strip() \
            .str.replace('\s+', '_')
        column_map = dict(zip(self.ret_df.columns, new_columns))
        self.ret_df.rename(columns=column_map, inplace=True, errors='raise')

        # re-order some of the columns in pandas
        first_cols = ['ticker', 'download_timestamp', 'date']
        last_cols = [col for col in self.ret_df.columns if col not in first_cols]
        self.ret_df = self.ret_df[first_cols + last_cols]

        return self

    def compute_returns_df(self):
        # convert from string to numeric if the column only contains numbers
        self.ret_df = self.ret_df.apply(pd.to_numeric, errors='ignore')

        # check for zero prices in the two fields used below for return computation
        # if there are zero prices, write out ticker to log file and continue to process
        zp_flag = ((self.ret_df['close'] <= 0).any()) or ((self.ret_df['adjusted_close'] <= 0).any())
        if zp_flag:
            log.error(f'zero [close or adjusted close] prices found for ticker {self.ticker}: '
                      f'will continue to process')

        # TODO: apply format to divyld and returns columns - both have NaN and could have inf as well
        # dividends are reported in unadjusted dollar amounts
        # compute dividend yield by dividing current-period dividend by prior-period unadjusted closing price
        self.ret_df['divyld'] = self.ret_df.dividend_amount / self.ret_df.close.shift(1)

        # total return computation: compute the relative price change as the ratio of adjacent
        # adjusted prices and add dividend yield
        self.ret_df['return'] = self.ret_df.adjusted_close.pct_change(periods=1) + self.ret_df['divyld']

        # column contents
        log.info(f'returns successfully computed prior to s3 upload')

        return self

    def put_returns_in_s3(self):

        s3_key = f'type=pricing/state=formatted/period={self.period.lower()}/' \
                 f'rundate={self.rundate}/ticker={self.ticker}.csv'
        csv_buffer = StringIO()
        self.ret_df.to_csv(csv_buffer, index=False)
        self.s3_response = self.s3_client.put_object(Bucket=config.AlphaVantage.S3_PRICING_BUCKET,
                                                     Key=s3_key,
                                                     Body=csv_buffer.getvalue())
        s3_output_url = f'{config.AlphaVantage.S3_OBJECT_ROOT}/{config.AlphaVantage.S3_PRICING_BUCKET}/{s3_key}'
        log.info(f's3 upload success: {s3_output_url}')
        return self

    @classmethod
    def put_invalid_tickers_in_s3(cls, rundate):
        if len(cls.invalid_tickers) > 0:
            timestamp = datetime.now()
            s3_key = f'type=pricing/state=invalid_tickers/rundate={rundate}/' \
                     f'{timestamp.strftime("%Y%m%d%H%M%S")}.csv'

            tickers_df = pd.DataFrame({'ticker': cls.invalid_tickers})
            tickers_df['upload_timestamp'] = timestamp.strftime("%Y-%m-%d %H:%M:%S")

            csv_buffer = StringIO()
            tickers_df.to_csv(csv_buffer, index=False)

            s3_client = aws_session.client('s3', region_name=config.AlphaVantage.S3_REGION_NAME)
            s3_client.put_object(Bucket=config.AlphaVantage.S3_PRICING_BUCKET,
                                 Key=s3_key,
                                 Body=csv_buffer.getvalue())
            s3_output_url = f'{config.AlphaVantage.S3_OBJECT_ROOT}/{config.AlphaVantage.S3_PRICING_BUCKET}/{s3_key}'
            log.info(f'uploaded invalid tickers to: {s3_output_url}')


def process_returns(ticker, period, rundate, s3_upload=True):
    returns = ReturnsProcessor(ticker, period, rundate)  # initialize returns object
    returns.request_prices()  # make an api call to download prices - cycle through api keys if needed

    if ReturnsProcessor.api_keys_exausted:
        ReturnsProcessor.put_invalid_tickers_in_s3(rundate)
        log.info(f'all available api keys used, exiting program')
        exit()

    if returns.validate_price_object() is True:
        returns.load_prices_into_df() \
            .compute_returns_df()

        if s3_upload:
            returns.put_returns_in_s3()

        return returns.ret_df


def build_ticker_download_list(etf_ticker, period, rundate, overwrite=False, remove_bad_tickers=True):
    # read the files containing index constituents and extract the list of unique tickers

    query_str = ('select distinct Ticker from qcdb.ishares_holdings '
                 'where '
                 'etf='f'\'{etf_ticker}\' '
                 'and '
                 'asofdate <= date 'f'\'{rundate}\''
                 'and Ticker <> \'\'')

    if remove_bad_tickers:
        query_str += ' and Ticker not in (select distinct Ticker from qcdb.av_invalid_tickers)'

    tickers_df = athena_helpers.query(query_str)

    tickers = [str(ticker).upper() for ticker in tickers_df['Ticker'].tolist()]

    # if user wants to overwrite files, then use all tickers for download
    # if no overwrite, then read the S3 folder to get a list of tickers
    # already processed: tickers_to_download are the remaining tickers
    if overwrite:
        tickers_to_download = sorted(tickers)
    else:
        # list available returns files in s3 bucket
        existing_tickers = s3_helpers.list_keys(Bucket=config.AlphaVantage.S3_PRICING_BUCKET,
                                                Prefix=f'type=pricing/state=formatted/period={period.lower()}/' \
                                                       f'rundate={rundate}/ticker=',
                                                full_path=False,
                                                remove_ext=True)
        existing_tickers = [ticker.upper() for ticker in existing_tickers]
        unprocessed_tickers = sorted(list(set(tickers) - set(existing_tickers)))
        tickers_to_download = unprocessed_tickers

    log.info(f'queued {len(tickers_to_download)} holding dates for download')
    return tickers_to_download


def sleep_between_requests(key_usage_run, sleep_time=60):
    if key_usage_run % config.AlphaVantage.MAX_HITS_PER_MINUTE == 0:
        log.info(f'reached {config.AlphaVantage.MAX_HITS_PER_MINUTE}-requests-per-minute threshold; '
                 f'sleeping for {sleep_time} secs')
        time.sleep(sleep_time)


def main(etf_ticker, period, rundate, overwrite):
    tickers_to_download = build_ticker_download_list(etf_ticker, period, rundate, overwrite)
    for count, ticker in enumerate(tickers_to_download):
        process_returns(ticker, period, rundate, s3_upload=True)
    ReturnsProcessor.put_invalid_tickers_in_s3(rundate)

# everything below will only be executed if this script is called from the command line
# if this file is imported, nothing below will be executed
if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='Get returns history from av')
    parser.add_argument('etf_ticker', help='Enter ETF Ticker (the program will retrieve returns for its constituents)')
    parser.add_argument('period', help='Enter "daily" to retrieve daily returns and "monthly" to get monthly returns')
    parser.add_argument('rundate', help='Enter run date in yyyy-mm-dd format')
    parser.add_argument('--overwrite', help=f'Overwrite returns that have already been downloaded to S3',
                        action='store_true')
    args = parser.parse_args()

    # logging (will inherit log calls from utils.pricing and utils.s3_helpers)
    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'./logs/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - '
                               f' {args.etf_ticker} - {args.rundate} - overwrite = {args.overwrite} - %(message)s')

    # run main
    main(args.etf_ticker, args.period, args.rundate, args.overwrite)
    log.info(f'successfully completed script')
