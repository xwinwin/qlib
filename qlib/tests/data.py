# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import re
import sys
import qlib
import shutil
import tarfile
import zipfile
import requests
import datetime
from tqdm import tqdm
from pathlib import Path
from loguru import logger
from qlib.utils import exists_qlib_data


class GetData:
    REMOTE_DATA_URL = "https://github.com/SunsetWolf/qlib_dataset/releases/download"
    REMOTE_BIN_URL = "https://github.com/chenditc/investment_data/releases"

    def __init__(self, delete_zip_file=False):
        """

        Parameters
        ----------
        delete_zip_file : bool, optional
            Whether to delete the zip file, value from True or False, by default False
        """
        self.delete_zip_file = delete_zip_file

    def merge_remote_url(self, base_url: str, file_name: str):
        """
        Generate download links.

        Parameters
        ----------
        file_name: str
            The name of the file to be downloaded.
            The file name can be accompanied by a version number, (e.g.: v2/qlib_data_simple_cn_1d_latest.zip),
            if no version number is attached, it will be downloaded from v0 by default.
        """
        return f"{base_url}/{file_name}" if "/" in file_name else f"{base_url}/v0/{file_name}"

    def download(self, url: str, target_path: Path, force_download: bool):
        """
        Download a file from the specified url.

        Parameters
        ----------
        url: str
            The url of the data.
        target_path: str
            The location where the data is saved, including the file name.
        """
        if not target_path.exists() or force_download:
            file_name = str(target_path).rsplit("/", maxsplit=1)[-1]
            resp = requests.get(url, stream=True, timeout=60)
            resp.raise_for_status()
            if resp.status_code != 200:
                raise requests.exceptions.HTTPError()

            chunk_size = 1024
            logger.warning(
                f"The data for the example is collected from Yahoo Finance. Please be aware that the quality of the data might not be perfect. (You can refer to the original data source: https://finance.yahoo.com/lookup.)"
            )
            logger.info(f"{os.path.basename(file_name)} downloading......")
            with tqdm(total=int(resp.headers.get("Content-Length", 0))) as p_bar:
                with target_path.open("wb") as fp:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        fp.write(chunk)
                        p_bar.update(chunk_size)

    def _get_bin_file_name_with_version(self, name, bin_version, dataset_version):
        if bin_version == "latest":
            # "https://github.com/chenditc/investment_data/releases/latest/download/qlib_bin.tar.gz"
            return  f"{bin_version}/download/{name}.tar.gz"
        else:
            # "https://github.com/chenditc/investment_data/releases/download/2020-10-11/qlib_bin.tar.gz"
            return f"download/{bin_version}/{name}.tar.gz"

    def _get_data_file_name_with_version(self, name, data_version, dataset_version):
        dataset_version = "v2" if dataset_version is None else dataset_version
        file_name_with_version = f"{dataset_version}/{name}_{self.region.lower()}_{self.interval.lower()}_{data_version}.zip"
        return file_name_with_version

    def download_data(self, name: str, target_dir: Path|str, delete_old: bool = True, force_download=True):
        """
        Download the specified file to the target folder.

        Parameters
        ----------
        target_dir: str
            data save directory
        file_name: str
            dataset name, needs to endwith .zip, value from [rl_data.zip, csv_data_cn.zip, ...]
            may contain folder names, for example: v2/qlib_data_simple_cn_1d_latest.zip
        delete_old: bool
            delete an existing directory, by default True
        force_download: bool
            download target file in force, by default True

        Examples
        ---------
        # get rl data
        python get_data.py download_data --file_name rl_data.zip --target_dir ~/.qlib/qlib_data/rl_data
        When this command is run, the data will be downloaded from this link: https://qlibpublic.blob.core.windows.net/data/default/stock_data/rl_data.zip?{token}

        # get cn csv data
        python get_data.py download_data --file_name csv_data_cn.zip --target_dir ~/.qlib/csv_data/cn_data
        When this command is run, the data will be downloaded from this link: https://qlibpublic.blob.core.windows.net/data/default/stock_data/csv_data_cn.zip?{token}
        -------

        """
        target_dir = Path(target_dir).expanduser()
        target_dir.mkdir(exist_ok=True, parents=True)
        get_file_name_with_version =  None
        data_version: str = ''
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        base_url: str = ''

        if name == "qlib_bin":
            data_version = today_str
            get_file_name_with_version = self._get_bin_file_name_with_version
            force_download = False
            base_url = self.REMOTE_BIN_URL
        else:
            data_version = ".".join(re.findall(r"(\d+)\.+", qlib.__version__))
            get_file_name_with_version = self._get_data_file_name_with_version
            base_url = self.REMOTE_DATA_URL

        url_path_file_name = get_file_name_with_version(name, data_version, dataset_version=self.version)
        url = self.merge_remote_url(base_url, url_path_file_name)
        target_file_name = f'{today_str}_{os.path.basename(url_path_file_name)}'
        if not self.check_dataset(url):
            force_download = True
            url_path_file_name = get_file_name_with_version(name, "latest", dataset_version=self.version)
            url = self.merge_remote_url(base_url, url_path_file_name)
            target_file_name = f'latest_{os.path.basename(url_path_file_name)}'

        # saved file name
        target_path = target_dir.joinpath(target_file_name)

        self.download(url=url, target_path=target_path, force_download=force_download)

        self._unzip(target_path, target_dir, delete_old)
        if self.delete_zip_file:
            target_path.unlink()

    def check_dataset(self, file_url: str):
        resp = requests.get(file_url, stream=True, timeout=60)
        status = True
        if resp.status_code == 404:
            status = False
        return status

    @staticmethod
    def _unzip(file_path: Path, target_dir: Path, delete_old: bool = True):
        file_path = Path(file_path)
        target_dir = Path(target_dir)
        if delete_old:
            logger.warning(
                f"will delete the old qlib data directory(features, instruments, calendars, features_cache, dataset_cache): {target_dir}"
            )
            GetData._delete_qlib_data(target_dir)
        logger.info(f"{file_path} unzipping......")
        if file_path.suffix == ".gz":
            # extract tar.gz file with strip-components=1 flag
            with tarfile.open(str(file_path.resolve()), "r:gz") as tf:
                members = tf.getmembers()
                for member in tqdm(members):
                    member.path = "/".join(member.path.split("/")[1:])  # strip-components=1
                    tf.extract(member, str(target_dir.resolve()))
        elif file_path.suffix == ".zip":
            with zipfile.ZipFile(str(file_path.resolve()), "r") as zp:
                for _file in tqdm(zp.namelist()):
                    zp.extract(_file, str(target_dir.resolve()))

    @staticmethod
    def _delete_qlib_data(file_dir: Path):
        rm_dirs = []
        for _name in ["features", "calendars", "instruments", "features_cache", "dataset_cache"]:
            _p = file_dir.joinpath(_name)
            if _p.exists():
                rm_dirs.append(str(_p.resolve()))
        if rm_dirs:
            flag = input(
                f"Will be deleted: "
                f"\n\t{rm_dirs}"
                f"\nIf you do not need to delete {file_dir}, please change the <--target_dir>"
                f"\nAre you sure you want to delete, yes(Y/y), no (N/n):"
            )
            if str(flag) not in ["Y", "y"]:
                sys.exit()
            for _p in rm_dirs:
                logger.warning(f"delete: {_p}")
                shutil.rmtree(_p)

    def qlib_data(
        self,
        name="qlib_data",
        target_dir="~/.qlib/qlib_data/cn_data",
        version=None,
        interval="1d",
        region="cn",
        delete_old=True,
        exists_skip=False,
        force_download=True,
    ):
        """download cn qlib data from remote

        Parameters
        ----------
        target_dir: str
            data save directory
        name: str
            dataset name, value from [qlib_data, qlib_data_simple], by default qlib_data
        version: str
            data version, value from [v1, ...], by default None(use script to specify version)
        interval: str
            data freq, value from [1d], by default 1d
        region: str
            data region, value from [cn, us], by default cn
        delete_old: bool
            delete an existing directory, by default True
        exists_skip: bool
            exists skip, by default False

        Examples
        ---------
        # get 1d data
        python get_data.py qlib_data --name qlib_data --target_dir ~/.qlib/qlib_data/cn_data --interval 1d --region cn
        When this command is run, the data will be downloaded from this link: https://qlibpublic.blob.core.windows.net/data/default/stock_data/v2/qlib_data_cn_1d_latest.zip?{token}

        # get 1min data
        python get_data.py qlib_data --name qlib_data --target_dir ~/.qlib/qlib_data/cn_data_1min --interval 1min --region cn
        When this command is run, the data will be downloaded from this link: https://qlibpublic.blob.core.windows.net/data/default/stock_data/v2/qlib_data_cn_1min_latest.zip?{token}

        # get qlib_bin data
        python get_data.py qlib_data --name qlib_bin --target_dir ~/.qlib/qlib_data/cn_data
        When this command is run, the data will be downloaded from this link: https://github.com/chenditc/investment_data/releases

        -------

        """
        if exists_skip and exists_qlib_data(target_dir):
            logger.warning(
                f"Data already exists: {target_dir}, the data download will be skipped\n"
                f"\tIf downloading is required: `exists_skip=False` or `change target_dir`"
            )
            return

        self.version = version
        self.interval = interval
        self.region = region

        self.download_data(name.lower(), target_dir, delete_old, force_download)
