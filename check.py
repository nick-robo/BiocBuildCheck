# %%

from typing import Optional, Iterable

import re

import bs4
import requests
import pandas as pd


def build_urls(package: Optional[str] = None, release: bool = True, devel: bool = False, path: str = "") -> list[str]:

    base = "https://bioconductor.org/checkResults"
    subfolder = "bioc-LATEST"

    filter_list = [release, devel]
    releases = ["release", "devel"]
    releases = [x for x, y in zip(releases, filter_list) if y]

    if not package:
        return ["/".join([base, release, subfolder]) + "/" for release in releases]

    return ["/".join([base, *releases, subfolder, package]) + '/' + path]


def parse_log(log: str, status: str) -> list[str]:

    status = status if status != "WARNINGS" else "WARNING"

    log_array = list(filter(lambda x: not "DONE" in x,
                     filter(lambda x: status in x, log.split("*"))))

    return log_array


def get_pages_data(package: Optional[str] = None, release: bool = True, devel: bool = False, path: str = "") -> list[bs4.BeautifulSoup]:

    urls = build_urls(package=package, release=release, devel=devel, path=path)

    pages_data = []

    for url in urls:
        page = requests.get(url, timeout=5)
        if not page.ok:
            raise Exception(f"Couldn't fetch url: {url}")
        pages_data.append(bs4.BeautifulSoup(page.text, features="lxml"))

    return pages_data


def get_package_status(packages: Optional[Iterable[str]] = None, devel: bool = False) -> pd.DataFrame:

    pages_data = get_pages_data(devel=devel)

    # if `packages` is None read packages file
    # TODO: make this more flexible
    if not packages:
        with open("packages", "r") as file:
            packages = file.read().splitlines()

    status = {name: [] for name in packages}

    for data in pages_data:
        for name in packages:
            links = data.find_all("a")
            package_row = list(filter(lambda x: x.text == name, links))

            if package_row:
                status[name].append(package_row[0].find_parent(
                    class_="gcard").get("class")[-1].upper())
            else:
                status[name].append("NOT FOUND")

    status_df = pd.DataFrame(status).T

    if devel:
        status_df.columns = ["release", "devel"]
    else:
        status_df.columns = ["release"]

    status_df = pd.melt(status_df.reset_index(), id_vars=['index'])

    return status_df


def get_info(status_df: pd.DataFrame) -> None:

    stage_dict = {
        'install': 'install',
        'buildsrc': "build",
        'checksrc': "check",
        'buildbin': 'bin'
    }

    for idx, (name, release, status, *_) in status_df.iterrows():
        # check input data are correct data type
        if not (isinstance(name, str) and isinstance(release, str) and isinstance(status, str)):
            raise Exception("Invalid data in status df.")

        if status == "OK" or status == "NOT FOUND":
            status_df.loc[idx, "stage"] = pd.NA  # type: ignore
            status_df.loc[idx, "message_count"] = 0  # type: ignore

            continue

        is_release = release == "release"

        data = get_pages_data(
            package=name, release=is_release, devel=not is_release)[0]

        error = data.find(class_=status)

        if not error:
            raise Exception(
                f"Could not find error path.\t\nName: {name}\t\nRelease: {release}")

        # check if the `error`'s parent is None. if it is not get the href
        error_path = error.parent.get("href") if error.parent else None

        # check if the `error_path` is a str
        error_path = error_path if isinstance(error_path, str) else None

        if not error_path:
            status_df.loc[idx, "stage"] = "pre-build"  # type: ignore
            status_df.loc[idx, "message_count"] = 1  # type: ignore
            status_df.loc[idx, "Message 1"] = error.parent.text.strip().split(  # type: ignore
                "(")[-1][:-1]
            continue

        # determine error stage
        stage = stage_dict[re.split(r"-|\.", error_path)[-2]]

        data = get_pages_data(package=name, release=is_release,
                              devel=not is_release, path=error_path)[0]

        log = pre.text.replace('â', "'") if (pre := data.find("pre")) else None

        if not log:
            raise Exception("Could not find error/worning log.")

        log_messages = parse_log(log, status)

        message_count = len(log_messages)

        status_df.loc[idx, "stage"] = stage  # type: ignore
        status_df.loc[idx, "message_count"] = message_count  # type: ignore
        for i, message in enumerate(log_messages):
            status_df.loc[idx, f"Message {i+1}"] = message  # type: ignore

    status_df.fillna(pd.NA, inplace=True)

    pretty_names = ["Name", "Release", "Log Level", "Stage", "Message Count"]
    status_df.rename(
        columns={key: value for key, value in zip(
            status_df.columns[:6], pretty_names)},
        inplace=True)
# %%

if __name__ == "__main__":
    df = get_package_status(["BiocCheck", "S4Vectors"], devel=True)
    get_info(df)
    pd.to_pickle(df, "saved.pkl")
