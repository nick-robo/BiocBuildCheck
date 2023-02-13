"""Depracated functions that are no longer maintained."""

import re
from typing import Iterable, Optional
from warnings import warn

import pandas as pd

from check import get_pages_data, parse_log, stage_dict


def get_package_status_old(
    packages: Optional[Iterable[str]] = None, devel: bool = False
) -> pd.DataFrame:
    """Get the status of each package and forms a data frame.

    Args:
        packages (Optional[Iterable[str]], optional):
            A list of packages of interest. Defaults to None.
        devel (bool, optional):
            Whether to get devel status. Defaults to False.

    Returns:
        pd.DataFrame: A data frame containing the status of the packages.
    """
    warn("Use get_package_status instead!", DeprecationWarning)

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
                filter(lambda x: x.text == name, links))

            if package_row:
                # get the name of the last class of the link's row (gcard)
                # NOTE: the classes are "compact gcard" followed by the status
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


def get_info(status_df: pd.DataFrame) -> None:
    """Populate the status data frame with detailed information.

    Args:
        status_df (pd.DataFrame): A data frame created by `get_package_status`.
    """
    warn("Use get_package_status instead!", DeprecationWarning)

    for idx, (name, release, status, *_) in status_df.iterrows():
        # check input data are correct data type

        if not all(isinstance(x, str) for x in [name, release, status]):
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
                "Could not find error path.\t\n",
                f"Name: {name}\t\nRelease: {release}")

        # check if the `error`'s parent is None. if it is not get the href
        log_link = error.parent.get("href") if error.parent else None

        # check if the `error_path` is a str
        log_link = log_link if isinstance(log_link, str) else None

        # deal with cases where package fails a pre-build check (no log)
        if not log_link:
            status_df.loc[idx, "stage"] = "pre-build"  # type: ignore
            status_df.loc[idx, "message_count"] = 1  # type: ignore
            status_df.loc[
                idx, "Message 1"  # type: ignore
            ] = error.parent.text.strip().split("(")[-1][:-1]
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
