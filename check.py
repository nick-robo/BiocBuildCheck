"""Functions for scraping and parsing Bioconductor build reports.
"""
# %%

import re
import os
from datetime import datetime
from typing import Iterable, Optional
from urllib.parse import urlparse

import bs4
import pandas as pd
import requests
from github import Github
from github.Issue import Issue

stage_dict = {
    'install': 'install',
    'buildsrc': "build",
    'checksrc': "check",
    'buildbin': 'bin'
}


def build_urls(
    package: str = "",
    release: bool = True,
    devel: bool = False,
    path: str = "",
    long: bool = False
) -> list[str]:
    """Build the URLs to request.

    Args:
        package (Optional[str], optional): Package of interest. Defaults to None.
        release (bool, optional): Whether to build release URLs. Defaults to True.
        devel (bool, optional): Whether to build devel URLs. Defaults to False.
        path (str, optional): A URL path to append to the URLs (e.g. "index.html"). Defaults to "".

    Returns:
        list[str]: _description_
    """
    base = "https://bioconductor.org/checkResults"
    subfolder = "bioc-LATEST"
    long_report = "https://bioconductor.org/checkResults/{}/bioc-LATEST/long-report.html"

    filter_list = [release, devel]
    releases = ["release", "devel"]
    releases = [x for x, y in zip(releases, filter_list) if y]

    if long:
        return [long_report.format(r) for r in releases]

    if not package and not path:
        return ["/".join([base, release, subfolder]) + "/" for release in releases]

    return ["/".join([base, *releases, subfolder, package]) + '/' + path.strip(".")]


def parse_log(log: str, status: str) -> list[str]:
    """Parse build and check logs for relelvant messages.

    Args:
        log (str): The build/check log.
        status (str): The log level (i.e. "ERROR", "WARNINGS").

    Returns:
        list[str]: A list of relevant messages from the log.
    """
    status = status if status != "WARNINGS" else "WARNING"

    log_array = list(filter(lambda x: not "DONE" in x,
                     filter(lambda x: status in x, log.split("*"))))

    return log_array


def get_pages_data(
    package: str = "",
    release: bool = True,
    devel: bool = False,
    path: str = "",
    long: bool = False
) -> list[bs4.BeautifulSoup]:
    """Gets the (HTML) page data of interest.

    Args:
        package (Optional[str], optional): A package of interest. Defaults to None.
        release (bool, optional): Whether to build release URLs. Defaults to True.
        devel (bool, optional): Whether to build devel URLs. Defaults to False.
        path (str, optional): A URL path to append to the URLs (e.g. "index.html"). Defaults to "".

    Raises:
        Exception: Failure to fetch the URL.

    Returns:
        list[bs4.BeautifulSoup]: _description_
    """

    urls = build_urls(package=package, release=release,
                      devel=devel, path=path, long=long)

    pages_data = []

    for url in urls:
        page = requests.get(url, timeout=5)
        if not page.ok:
            raise Exception(f"Couldn't fetch url: {url}")
        pages_data.append(bs4.BeautifulSoup(page.text, features="lxml"))

    return pages_data


def get_package_status(
    packages: Optional[Iterable[str]] = None, devel: bool = False
) -> pd.DataFrame:
    """Gets the status of each package and forms a data frame.

    Args:
        packages (Optional[Iterable[str]], optional): A list of packages of interest.
            Defaults to None.
        devel (bool, optional): Whether to get devel status. Defaults to False.

    Returns:
        pd.DataFrame: A data frame containing the status of the packages.
    """

    pages_data = get_pages_data(devel=devel)

    # if `packages` is None read packages file
    if not packages:
        with open("packages", "r", encoding="utf-8") as file:
            packages = file.read().splitlines()

    status = {name: [] for name in packages}

    # loop through release and devel
    for data in pages_data:
        for name in packages:
            # get all hyperlinks
            links = data.find_all("a")
            # get the hyperlink who's text is the same as the package name
            package_row = list(
                filter(lambda x: x.text == name, links))  # pylint: disable=cell-var-from-loop

            if package_row:
                # get the name of the last class of the link's row (gcard)
                # NOTE: the classes consist of "compact gcard" followed by the status
                # or statuses  (e.g. "compact gcard timeout warnings")
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


def get_log_messages(log_link: str, name: str, is_release: bool, status: str) -> list[str]:

    data = get_pages_data(release=is_release,
                          devel=not is_release, path=log_link)[0]

    log = pre.text.replace('â', "'") if (pre := data.find("pre")) else None

    if not log:
        raise Exception("Could not find error/warning log.")

    return parse_log(log, status)


def get_package_status_v2(
        packages: Optional[Iterable[str]] = None,
        devel: bool = False,
        pages_data: Optional[Iterable[bs4.BeautifulSoup]] = None
) -> pd.DataFrame:

    if not pages_data:
        pages_data = get_pages_data(devel=devel, long=True)

    if not packages:
        with open("packages", "r", encoding="utf-8") as file:
            packages = file.read().splitlines()

    status_dict = {}
    col_names = ["Name", "Release", "Version", "Maintainer",
                 "Log Level", "Stage", "Message Count"]

    releases = ["release", "devel"] if devel else ['release']

    i = None
    max_message_count = 0

    # iterate through the retrieved release logs
    for release, soup in zip(releases, pages_data):
        # find each package link
        package_dict = {
            l.text: l for l in soup.find_all("a")
            if l and (href := l.get("href"))
            and "." in href
            and l.text in packages
        }
        is_release = release == "release"

        # for each requested package
        for name in packages:
            i = 0 if i is None else i + 1

            if not name in package_dict.keys():
                status_dict[i] = (name, release, pd.NA, pd.NA, "NOT FOUND", pd.NA)
                continue

            # get the card class, a container for all details about the build
            link = package_dict[name]
            card = link.find_parent(class_="gcard")

            # get package information
            version = link.parent.text.split("\xa0")[-1]
            maintainer = link.parent.find_next_sibling("br").next

            # get the classes of card less "gcard".
            # the will be in ("ok", "warning", "error", "timeout")
            status_list = card.get("class")[1:]

            # for each package status
            for status in status_list:
                if status == "ok":
                    status_dict[i] = (name, release, version, maintainer, "OK")
                    break

                log_link = card.find(class_=status.upper()).parent.get("href")

                if not log_link:
                    status_dict[i] = (name, release, version, maintainer, "pre-build",
                                      1, card.find(class_=status.upper()).parent.text)
                    max_message_count = max(1, max_message_count)

                stage = stage_dict[re.split(r"-|\.", log_link)[-2]]
                status = status.upper()
                # get log information
                messages = get_log_messages(log_link, name, is_release, status)
                message_count = len(messages)
                max_message_count = max(message_count, max_message_count)

                status_dict[i] = (
                    name, release, version, maintainer, status, stage, message_count, *messages
                )

    col_names.extend(["Message " + str(j + 1)
                     for j in range(max_message_count)])

    data = pd.DataFrame.from_dict(
        status_dict, orient="index", columns=col_names)

    return data


def get_info(status_df: pd.DataFrame) -> None:
    """Populates the status data frame with detailed information.

    Args:
        status_df (pd.DataFrame): A data frame constructed by `get_package_status`.
    """

    for idx, (name, release, status, *_) in status_df.iterrows():
        # check input data are correct data type
        if not (isinstance(name, str) and isinstance(release, str) and isinstance(status, str)):
            raise ValueError("Invalid data in status df.")

        if status in ('OK', 'NOT FOUND'):
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
        log_link = error.parent.get("href") if error.parent else None

        # check if the `error_path` is a str
        log_link = log_link if isinstance(log_link, str) else None

        # deal with cases where package fails a pre-build check (no log)
        if not log_link:
            status_df.loc[idx, "stage"] = "pre-build"  # type: ignore
            status_df.loc[idx, "message_count"] = 1  # type: ignore
            status_df.loc[idx, "Message 1"] = error.parent.text.strip().split(  # type: ignore
                "(")[-1][:-1]
            continue

        # determine error stage from the error path
        stage = stage_dict[re.split(r"-|\.", log_link)[-2]]

        # get the log URL
        data = get_pages_data(package=name, release=is_release,
                              devel=not is_release, path=log_link)[0]

        log = pre.text.replace('â', "'") if (pre := data.find("pre")) else None

        if not log:
            raise Exception("Could not find error/warning log.")

        log = parse_log(log, status)

        status_df.loc[idx, "stage"] = stage  # type: ignore
        status_df.loc[idx, "message_count"] = len(log)  # type: ignore
        for i, message in enumerate(log):
            status_df.loc[idx, f"Message {i+1}"] = message  # type: ignore

    status_df.fillna(pd.NA, inplace=True)

    pretty_names = ["Name", "Release", "Log Level", "Stage", "Message Count"]
    status_df.rename(
        columns=dict(zip(status_df.columns[:6], pretty_names)),
        inplace=True)


def get_download_stats(status_df: pd.DataFrame) -> pd.DataFrame:

    def data_url(name: str) -> str:
        return "https://bioconductor.org/packages/stats/bioc/" + name + "/" + name + "_stats.tab"

    dfs = []
    for name in status_df.iloc[:, 0].unique():
        now = datetime.now()  # pylint: disable=unused-variable
        dfs.append(
            pd.read_csv(data_url(name), delimiter="\t")
            .query("Month != 'all'")  # remove month totals
            .drop("Nb_of_distinct_IPs", axis=1)  # drop distinct IPs
            .assign(Name=name)  # create column with package name
            # reorder columns and create `datetime` "Date" columns
            .pipe(lambda df: pd.DataFrame({
                "Name": df.Name,
                "Date": pd.to_datetime(df.Year.astype('str') + "-" + df.Month),
                "Downloads": df.Nb_of_downloads}))
            .query("Date < @now")  # remove dates in the future
        )

    return pd.concat(dfs)


def get_descrption_data(name: str) -> dict[str, str]:

    base = "https://bioconductor.org/packages/release/bioc/html/"

    data = pd.read_html(base + name + ".html",
                        attrs={"class": "details"})[0].fillna("")
    data.columns = ["key", "value"]

    return data.set_index("key").to_dict()["value"]


def get_issues(status_df: pd.DataFrame) -> dict[str, Optional[tuple[Issue]]]:

    if not (pat := os.environ.get("GITHUB_PAT")):
        with open("pat", encoding="UTF8") as file:
            pat = file.read().splitlines()[0]

    result = {}
    github = Github(pat)

    for name in status_df.iloc[:, 0].unique():
        data = get_descrption_data(name)

        if not ("BugReports" in data.keys()) or not data["BugReports"]:
            result[name] = None
            continue

        url = urlparse(data["BugReports"])

        # 1. split the path: "/Org/Repo/issues" -> ["", "Org", "Repo", "Issues"]
        # 2. filter: ["", "Org", "Repo", "Issues"] -> ["Org", "Repo"]
        # 3. join to string: ["Org", "Repo"] -> "Org/Repo"
        repo_name = "/".join(x for x in url.path.split("/")
                             if x and x != "issues")

        result[name] = tuple(github.get_repo(
            repo_name).get_issues(state="open"))

    return result
# %%


if __name__ == "__main__":
    df = get_package_status_v2(devel=True)
    # get_info(df)
    # pd.to_pickle(df, "saved.pkl")
    # issues = get_issues(df)

    print(df)


# %%
