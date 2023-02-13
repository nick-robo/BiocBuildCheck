"""Functions for scraping and parsing Bioconductor build reports."""
# %%

import os
import re
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
        package (Optional[str], optional):
            Package of interest. Defaults to None.
        release (bool, optional):
            Whether to build release URLs. Defaults to True.
        devel (bool, optional):
            Whether to build devel URLs. Defaults to False.
        path (str, optional):
            A URL path to append to the URLs (e.g. "index.html").
            Defaults to "".

    Returns:
        list[str]: _description_
    """
    base = "https://bioconductor.org/checkResults"
    subdir = "bioc-LATEST"
    long_report = base + "/{}/bioc-LATEST/long-report.html"

    filter_list = [release, devel]
    releases = ["release", "devel"]
    releases = [x for x, y in zip(releases, filter_list) if y]

    if long:
        return [long_report.format(r) for r in releases]

    if not package and not path:
        return [
            "/".join([base, release, subdir]) + "/" for release in releases
        ]

    return [
        "/".join([base, *releases, subdir, package]) + '/' + path.strip(".")
    ]


def parse_log(log: str, status: str) -> list[str]:
    """Parse build and check logs for relelvant messages.

    Args:
        log (str): The build/check log.
        status (str): The log level (i.e. "ERROR", "WARNINGS").

    Returns:
        list[str]: A list of relevant messages from the log.
    """
    status = status if status != "WARNINGS" else "WARNING"

    log_array = list(filter(lambda x: "DONE" not in x,
                     filter(lambda x: status in x, log.split("*"))))

    return log_array


def get_pages_data(
    package: str = "",
    release: bool = True,
    devel: bool = False,
    path: str = "",
    long: bool = False
) -> list[bs4.BeautifulSoup]:
    """Get the (HTML) page data of interest.

    Args:
        package (Optional[str], optional):
            A package of interest. Defaults to None.
        release (bool, optional):
            Whether to build release URLs. Defaults to True.
        devel (bool, optional):
            Whether to build devel URLs. Defaults to False.
        path (str, optional):
        A URL path to append to the URLs (e.g. "index.html"). Defaults to "".

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


def get_log_messages(
    log_link: str,
    is_release: bool,
    status: str
) -> list[str]:
    """Get the log messages from a specified link.

    Args:
        log_link (str): Link to the log.
        is_release (bool): Flag indicating if link refers to release of devel.
        status (str): The log level of the error.

    Returns:
        list[str]: A list of log messages.
    """
    data = get_pages_data(release=is_release,
                          devel=not is_release, path=log_link)[0]

    log = pre.text.replace('â', "'") if (pre := data.find("pre")) else None

    if not log:
        raise Exception("Could not find error/warning log.")

    return parse_log(log, status)


def get_package_status(
        packages: Iterable[str],
        devel: bool = False,
        pages_data: Optional[Iterable[bs4.BeautifulSoup]] = None
) -> pd.DataFrame:
    """Build the package status data.

    Args:
        packages (Iterable[str]):
            Packages to build data for.
        devel (bool, optional):
            Whether to build data for the devel release. Defaults to False.
        pages_data (Iterable[bs4.BeautifulSoup], optional):
            Pre-queried page data. Defaults to None.

    Returns:
        pd.DataFrame: The package status data.
    """
    if not pages_data:
        pages_data = get_pages_data(devel=devel, long=True)

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
            link.text: link for link in soup.find_all("a")
            if link and (href := link.get("href"))
            and "." in href
            and link.text in packages
        }
        is_release = release == "release"

        # for each requested package
        for name in packages:
            i = 0 if i is None else i + 1

            if name not in package_dict.keys():
                status_dict[i] = (name, release, pd.NA,
                                  pd.NA, "NOT FOUND", pd.NA)
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
                    status_dict[i] = (name, release, version,
                                      maintainer, "OK", pd.NA, 0)
                    break

                log_link = card.find(class_=status.upper()).parent.get("href")

                if not log_link:
                    status_dict[i] = (
                        name, release, version, maintainer, "pre-build",
                        1, card.find(class_=status.upper()).parent.text
                    )
                    max_message_count = max(1, max_message_count)

                stage = stage_dict[re.split(r"-|\.", log_link)[-2]]
                status = status.upper()
                # get log information
                messages = get_log_messages(log_link, is_release, status)
                message_count = len(messages)
                max_message_count = max(message_count, max_message_count)

                status_dict[i] = (
                    name, release, version, maintainer, status, stage,
                    message_count, *messages
                )

    col_names.extend(["Message " + str(j + 1)
                     for j in range(max_message_count)])

    data = pd.DataFrame.from_dict(
        status_dict, orient="index", columns=col_names)

    return data


def get_download_stats(packages: pd.DataFrame | Iterable[str]) -> pd.DataFrame:
    """Get the package download stats.

    Args:
        packages (pd.DataFrame | Iterable[str]):
            Either status df created by `get_package_status` or a list of
            package names.

    Returns:
        pd.DataFrame: The package download stats.
    """
    def data_url(name: str) -> str:
        return "https://bioconductor.org/packages/stats/bioc/" \
            + name + "/" + name + "_stats.tab"

    if isinstance(packages, pd.DataFrame):
        package_names = packages.iloc[:, 0].unique()
    else:
        package_names = packages

    dfs = []
    for name in package_names:
        now = datetime.now()  # noqa: F841
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
    """Get the DESCRIPTION file data from Bioconductor.

    Args:
        name (str): The name of the package.

    Returns:
        dict[str, str]: The DESCRIPTION file as a dictionary.
    """
    base = "https://bioconductor.org/packages/release/bioc/html/"

    data = pd.read_html(base + name + ".html",
                        attrs={"class": "details"})[0].fillna("")
    data.columns = ["key", "value"]

    return data.set_index("key").to_dict()["value"]


def get_issues(
    packages: pd.DataFrame | Iterable[str]
) -> dict[str, Optional[tuple[Issue]]]:
    """Get the open issues from a packages' GitHub pages.

    Args:
        packages (pd.DataFrame | Iterable[str]): List of packages to query.

    Returns:
        dict[str, Optional[tuple[Issue]]]:
            A dict of each package with a list of issues.
    """
    if not (pat := os.environ.get("GITHUB_PAT")):
        with open("pat", encoding="UTF8") as file:
            pat = file.read().splitlines()[0]

    if isinstance(packages, pd.DataFrame):
        package_names = packages.iloc[:, 0].unique()
    else:
        package_names = packages

    result = {}
    github = Github(pat)

    for name in package_names:
        data = get_descrption_data(name)

        if not ("BugReports" in data.keys()) or not data["BugReports"]:
            result[name] = None
            continue

        url = urlparse(data["BugReports"])

        # 1. split the path: "/Org/Repo/issues" -> ["","Org","Repo","Issues"]
        # 2. filter: ["","Org","Repo","Issues"] -> ["Org","Repo"]
        # 3. join to string: ["Org","Repo"] -> "Org/Repo"
        repo_name = "/".join(x for x in url.path.split("/")
                             if x and x != "issues")

        result[name] = tuple(github.get_repo(
            repo_name).get_issues(state="open"))

    return result
# %%


if __name__ == "__main__":
    with open("packages", "r", encoding="utf-8") as package_file:
        sydneybiox_packages = package_file.read().splitlines()

    df = get_package_status(sydneybiox_packages, devel=True)
    # get_info(df)
    # pd.to_pickle(df, "saved.pkl")
    # issues = get_issues(df)

    print(df)
# %%
