"""Functions for scraping and parsing Bioconductor build reports."""
# %%

from genericpath import exists
import os
import pickle
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
    "install": "install",
    "buildsrc": "build",
    "checksrc": "check",
    "buildbin": "bin",
}


class BiocDownloadsError(ValueError):
    """Error for when all downloads queries fail."""

    pass


def build_urls(
    package: str = "",
    type: str = "Software",
    release: bool = True,
    devel: bool = False,
    path: str = "",
    long: bool = False,
) -> list[str]:
    """Build the URLs to request.

    Args:
        package (Optional[str], optional):
            Package of interest. Defaults to None.
        type (str, optional):
            A string indicating the package type, one of "Software", "Workflow",
            or "ExperimentData"
        release (bool, optional):
            Whether to build release URLs. Defaults to True.
        devel (bool, optional):
            Whether to build devel URLs. Defaults to False.
        path (str, optional):
            A URL path to append to the URLs (e.g. "index.html").
            Defaults to "".
        long (bool, optional):
            Whether to fetch the long report URL.

    Returns:
        list[str]: A list of URLs to be queried.
    """
    folders: dict[str, str] = {
        "Software": "bioc",
        "Workflow": "workflows",
        "ExperimentData": "data-experiment",
    }
    base = "https://bioconductor.org/checkResults"
    subdir = f"{folders[type]}-LATEST"
    long_report = (
        base + "/{}" + f"/{subdir}/{'long-report.html' if 'bioc' in subdir else ''}"
    )

    filter_list = [release, devel]
    releases = ["release", "devel"]
    releases = [x for x, y in zip(releases, filter_list) if y]

    if long:
        return [long_report.format(r) for r in releases]

    if not package and not path:
        return ["/".join([base, release, subdir]) + "/" for release in releases]

    return [
        "/".join([base, *releases, subdir, package]).rstrip("/") + "/" + path.strip(".")
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

    log_array = list(
        filter(
            lambda x: "DONE" not in x, filter(lambda x: status in x, log.split("\n*"))
        )
    )

    return log_array


def get_pages_data(
    package: str = "",
    type: str = "Software",
    release: bool = True,
    devel: bool = False,
    path: str = "",
    long: bool = False,
) -> list[bs4.BeautifulSoup]:
    """Get the (HTML) page data of interest.

    Args:
        package (str, optional):
            A package of interest. Defaults to None.
        type (str, optional):
            A string indicating the package type, one of "Software", "Workflow",
            or "ExperimentData"
        release (bool, optional):
            Whether to build release URLs. Defaults to True.
        devel (bool, optional):
            Whether to build devel URLs. Defaults to False.
        path (str, optional):
            A URL path to append to the URLs (e.g. "index.html"). Defaults to "".
        long (bool, optional):
            Whether to fetch the long report URL.

    Raises:
        Exception: Failure to fetch the URL.

    Returns:
        list[bs4.BeautifulSoup]: The page data (html) for the requested URLs.
    """
    urls = build_urls(
        package=package, type=type, release=release, devel=devel, path=path, long=long
    )

    pages_data = []

    for url in urls:
        page = requests.get(url, timeout=5)
        if not page.ok:
            raise Exception(f"Couldn't fetch url: {url}")
        pages_data.append(
            bs4.BeautifulSoup(page.content.decode("utf-8"), features="lxml")
        )

    return pages_data


def get_log_messages(
    log_link: str, is_release: bool, status: str, type: str
) -> list[str]:
    """Get the log messages from a specified link.

    Args:
        log_link (str): Link to the log.
        is_release (bool): Flag indicating if link refers to release of devel.
        status (str): The log level of the error.
        type (str): The software type.

    Returns:
        list[str]: A list of log messages.
    """
    data = get_pages_data(
        type=type, release=is_release, devel=not is_release, path=log_link
    )[0]

    log = pre.text.replace("â", "'") if (pre := data.find("pre")) else None

    if not log:
        raise Exception("Could not find error/warning log.")

    return parse_log(log, status)


def get_package_status(
    packages: pd.DataFrame,
    soups: dict[str, list[bs4.BeautifulSoup]],
    devel: bool = False,
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
    status_dict = {}
    col_names = [
        "Name",
        "Type",
        "Release",
        "Version",
        "Maintainer",
        "Log Level",
        "Stage",
        "Message Count",
    ]
    types: list[str] = list(packages.Type.unique())

    releases = (["release", "devel"] if devel else ["release"]) * len(types)

    i = None
    max_message_count = 0

    pages_data = [
        (soup, soup_type)
        for soup_type, soup_list in soups.items()
        if soup_type in types
        for keep, soup in zip([True, devel], soup_list)
        if keep
    ]

    # iterate through the retrieved release logs
    for release, (soup, soup_type) in zip(releases, pages_data):
        paks = list(packages.Name[packages.Type == soup_type])
        # find each package link
        if soup_type == "Software":  # only need to check "." in href for Software
            package_dict = {
                link.text: link
                for link in soup.find_all("a")
                if link
                and (href := link.get("href"))
                and "." in href
                and link.text in paks
            }
        else:
            package_dict = {
                link.text: link
                for link in soup.find_all("a")
                if link and link.get("href") and link.text in paks
            }

        is_release = release == "release"

        # for each requested package
        for name in paks:
            i = 0 if i is None else i + 1

            if name not in package_dict.keys():
                status_dict[i] = [name, release, pd.NA, pd.NA, "NOT FOUND", pd.NA, 0]
                continue

            # get the card class, a container for all details about the build
            link = package_dict[name]
            card = link.find_parent(class_="gcard")

            # get package information
            version = link.parent.text.split("\xa0")[-1]
            if soup_type == "Software":
                maintainer = link.parent.find_next_sibling("br").next
            else:
                maintainer = link.parent.find_next("td").next

            # get the classes of card less "gcard".
            # the will be in ("ok", "warning", "error", "timeout")
            status_list = card.get("class")[1:]

            # for each package status
            for status in status_list:
                if status == "gcard":
                    continue

                if status == "ok":
                    status_dict[i] = [
                        name,
                        soup_type,
                        release,
                        version,
                        maintainer,
                        "OK",
                        pd.NA,
                        0,
                    ]
                    break
                log_link = card.find(class_=status.upper()).parent.get("href")

                if not log_link:
                    status_dict[i] = [
                        name,
                        soup_type,
                        release,
                        version,
                        maintainer,
                        "pre-build",
                        1,
                        card.find(class_=status.upper()).parent.text,
                    ]
                    max_message_count = max(1, max_message_count)

                stage = stage_dict[re.split(r"-|\.", log_link)[-2]]
                status = status.upper()
                # get log information
                messages = get_log_messages(log_link, is_release, status, soup_type)
                message_count = len(messages)
                max_message_count = max(message_count, max_message_count)

                status_dict[i] = [
                    name,
                    soup_type,
                    release,
                    version,
                    maintainer,
                    status,
                    stage,
                    message_count,
                    *messages,
                ]

    col_names.extend(["Message " + str(j + 1) for j in range(max_message_count)])

    max_len = 7 + max_message_count

    for i in range(len(status_dict)):
        status_dict[i] += [pd.NA] * (max_len - len(status_dict[i]))

    data = pd.DataFrame.from_dict(status_dict, orient="index", columns=col_names)

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
        return (
            "https://bioconductor.org/packages/stats/bioc/"
            + name
            + "/"
            + name
            + "_stats.tab"
        )

    if isinstance(packages, pd.DataFrame):
        package_names = packages.iloc[:, 0].unique()
    else:
        package_names = packages

    dfs = []
    for name in package_names:
        now = datetime.now()  # noqa: F841
        try:
            query_df = pd.read_csv(data_url(name), delimiter="\t")
        except Exception:  # package has no download data
            continue
        dfs.append(
            query_df.query("Month != 'all'")  # remove month totals
            # .drop("Nb_of_distinct_IPs", axis=1)  # drop distinct IPs
            .assign(Name=name)  # create column with package name
            # reorder columns and create `datetime` "Date" columns
            .pipe(
                lambda df: pd.DataFrame(
                    {
                        "Name": df.Name,
                        "Date": pd.to_datetime(
                            df.Year.astype("str") + "-" + df.Month, format="%Y-%b"
                        ),
                        "Downloads": df.Nb_of_downloads,
                        "Distinct IPs": df.Nb_of_distinct_IPs,
                    }
                )
            )
            .query("Date < @now")  # remove dates in the future
        )

    if not dfs:
        raise BiocDownloadsError("None of the packages have any download data.")

    return pd.concat(dfs)


def get_descrption_data(name: str) -> dict[str, str]:
    """Get the DESCRIPTION file data from Bioconductor.

    Args:
        name (str): The name of the package.

    Returns:
        dict[str, str]: The DESCRIPTION file as a dictionary.
    """
    base = "https://bioconductor.org/packages/devel/bioc/html/"

    try:
        data = pd.read_html(base + name + ".html", attrs={"class": "details"})[
            0
        ].fillna("")
    except Exception as e:
        print(name)
        raise ValueError(f"Invalid URL: {base + name + '.html'}.", f"Error: {e}")
    data.columns = ["key", "value"]

    return data.set_index("key").to_dict()["value"]


def get_issues(
    packages: pd.DataFrame | Iterable[str],
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
        try:
            data = get_descrption_data(name)
        except Exception:  # package doesn't have a page yet
            result[name] = None
            continue

        if "BugReports" not in data.keys() or not data["BugReports"]:
            result[name] = None
            continue

        url = urlparse(data["BugReports"])

        # 1. split the path: "/Org/Repo/issues" -> ["","Org","Repo","Issues"]
        # 2. filter: ["","Org","Repo","Issues"] -> ["Org","Repo"]
        # 3. join to string: ["Org","Repo"] -> "Org/Repo"
        repo_name = "/".join(x for x in url.path.split("/") if x and x != "issues")

        result[name] = tuple(github.get_repo(repo_name).get_issues(state="open"))

    return result


def get_github_status(
    query_results: dict[str, Optional[tuple[Issue]]]
) -> tuple[pd.DataFrame | None, list[str], list[str]]:
    """Get the GitHub issue information for the dashboard.

    Generate a dataframe with the key issue characteristics and 2 lists containing
    the packages with no BugReport URLs and no issues respectively.

    Parameters
    ----------
    query_results : dict[str, Optional[tuple[Issue]]]
        The result of `get_issues`.

    Returns
    -------
    tuple[pd.DataFrame | None, list[str], list[str]]
        A DataFrame containing the issue details (None if no issues), a list of
        packages with no BugReport URL, and a list of packages with no issues.
    """
    detail_list = []
    missing_list = []
    no_issues = []

    for pak, issues in query_results.items():
        if issues is None:
            missing_list.append(pak)
            continue
        # if issues is empty
        if not issues:
            no_issues.append(pak)
            continue

        for issue in issues:
            detail_list.append(
                {
                    "Name": pak,
                    "Title": issue.title,
                    "Number": issue.number,
                    "Labelled": "Yes" if (
                        isLabled := bool(labs := issue.labels)) else "No",
                    "Bug": "Yes" if (
                        "bug" in labs[0].name if isLabled else False) else "No",
                    "Assigned": "Yes" if (bool(issue.assignee)) else "No",
                    "URL": issue.html_url,
                }
            )
    df = pd.DataFrame(detail_list) if detail_list else None

    return df, missing_list, no_issues


def get_package_list() -> pd.DataFrame:
    """Get a list of all Bioconductor packages with build reports.

    Returns
    -------
    pd.DataFrame
        A data frame containing each packages with it's corrisponding type, where type
        is one of Software, Workflow or ExperimentData.

    Raises
    ------
    Exception
        Failure to contact Bioconductor.
    """
    folders: dict[str, str] = {
        "bioc": "Software",
        "workflows": "Workflow",
        "data/experiment": "ExperimentData",
    }
    base = "https://bioconductor.org/packages/devel/"
    path = "/VIEWS"
    urls = [base + f + path for f in folders.keys()]
    paks: dict[str, str] = dict()

    for pak_type, url in zip(folders.values(), urls):
        page = requests.get(url, timeout=5)
        if not page.ok:
            raise Exception(f"Couldn't fetch url: {url}")

        data = bs4.BeautifulSoup(page.content.decode("utf-8"), features="lxml")

        text: str = data.find_all("p")[0].text
        p = [x.lstrip("Package:").strip() for x in text.split("\n") if "Package: " in x]
        paks.update(dict.fromkeys(p, pak_type))

    return pd.DataFrame({"Name": paks.keys(), "Type": paks.values()})


def __load_soups(dir: str = "cache") -> dict[str, list[bs4.BeautifulSoup]]:
    if exists("soup.pkl"):
        data: dict[str, list[bs4.BeautifulSoup]] = pickle.load(open("soup.pkl", "rb"))
    else:
        raise Exception("Pickle not found :(")
    return data


# %%


if __name__ == "__main__":
    data = __load_soups()  # load the pickle :)

    df = get_package_status(
        pd.DataFrame(
            {
                "Name": ["BiocGenerics", "beta7", "spicyWorkflow"],
                "Type": ["Software", "ExperimentData", "Workflow"],
            }
        ),
        soups=data,
        devel=True,
    )

    print(df)
# %%
