"""Runs a dashboard to visualise the build status of Bioconductor packages."""
# %%
from datetime import date
from os.path import exists, getmtime
from os import makedirs
from time import time
from typing import Iterable
from warnings import simplefilter

import altair as alt
import numpy as np
import pandas as pd
import plotly_express as px
import streamlit as st
from bs4 import BeautifulSoup
from github.Issue import Issue
from st_aggrid import AgGrid, AgGridReturn, ColumnsAutoSizeMode, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
from streamlit_plotly_events import plotly_events

from check import (
    get_download_stats,
    get_issues,
    get_package_status,
    get_pages_data,
    BiocDownloadsError,
    build_urls,
    get_package_list,
)

# ignore fufturewarning thrown by AgGrid
simplefilter("ignore", FutureWarning)


class DashData:
    """A container class for all the dash data which handles caching."""

    def __init__(self, package_list: Iterable[str] | None = None) -> None:
        """Create a DashData object.

        Args:
            packages (Iterable[str] | None, optional):
                A list of packages. Defaults to None (will load SydneyBioX
                packages).
        """
        # load soup
        self.soup: dict[str, list[BeautifulSoup]] = {
            "Software": [],
            "Workflow": [],
            "ExperimentData": [],
        }

        self.soup_age: float = float("inf")

        for type in self.soup.keys():
            # ensure that the cache path exists
            path = f"cache/{type}"
            makedirs(path, exist_ok=True)

            if exists(f"{path}/release.html") and exists(f"{path}/devel.html"):
                # get the oldest (minimum) time of last modification.
                mtime = min(
                    getmtime(f"{path}/release.html"), getmtime(f"{path}/devel.html")
                )

                # ensure it is not older than 8 hours.
                if (time() - mtime) / 3600 > 8:
                    self.update_soup(type)
                else:
                    self.soup_age = mtime if mtime < self.soup_age else self.soup_age
                    with open(f"{path}/release.html", "r") as rel:
                        self.soup[type].append(BeautifulSoup(rel, features="lxml"))
                    with open(f"{path}/devel.html", "r") as devel:
                        self.soup[type].append(BeautifulSoup(devel, features="lxml"))
            else:
                self.update_soup(type)

        if "pak_list" in st.session_state:
            self.valid_paks: pd.DataFrame = st.session_state["pak_list"]
        else:
            self.valid_paks = get_package_list()

        if not package_list:
            with open("packages", "r", encoding="utf-8") as file:
                self.packages = self.valid_paks[
                    self.valid_paks["Name"].isin(file.read().splitlines())
                ]
                self.sydneybiox_packs = self.packages.copy().drop_duplicates()
        else:
            self.packages = self.valid_paks[self.valid_paks["Name"].isin(package_list)]

        self.__status_df = get_package_status(
            packages=self.packages, devel=True, soups=self.soup
        )

        self.__downloads_age = None
        self.__downloads = None
        self.__github_age = None
        self.__github_issues = None

    def update_soup(self, type: str) -> None:
        """Update the Bioconductor build report data and store it."""
        self.soup[type] = get_pages_data(type=type, devel=True, long=True)
        self.soup_age = time()

        with open(f"cache/{type}/release.html", "w") as release:
            release.write(str(self.soup[type][0]))
        with open(f"cache/{type}/devel.html", "w") as devel:
            devel.write(str(self.soup[type][1]))

    def update_packages(self, package_list: Iterable[str]) -> None:
        """Update package list in data.

        Args:
            packages (Iterable[str]): New packages.
        """
        # if packages is empty
        if not package_list:
            if set(self.packages.Name) == set(self.sydneybiox_packs.Name):
                return

            self.packages = self.sydneybiox_packs
            self.reset_data()
            return

        # do nothing if packs unchanged
        if set(package_list) == set(list(self.packages.Name)):
            return

        self.packages = self.valid_paks[self.valid_paks["Name"].isin(package_list)]
        self.reset_data()

    def reset_data(self):
        """Reset stored data."""
        self.__downloads = None
        self.__github_issues = None
        self.__status_df = None

    @property
    def status_df(self) -> pd.DataFrame:
        """Get the status of the packages.

        Returns:
            pd.DataFrame: The package status data.
        """
        if (time() - self.soup_age) / 3600 > 8:
            for type in self.soup.keys():
                self.update_soup(type)

            self.__status_df = get_package_status(
                packages=self.packages, devel=True, soups=self.soup
            )
            return self.__status_df

        if self.__status_df is None:
            self.__status_df = get_package_status(
                packages=self.packages, devel=True, soups=self.soup
            )

        return self.__status_df

    @property
    def downloads(self) -> pd.DataFrame | None:
        """Get the download statitics of the packages.

        Returns:
            pd.DataFrame: The package download statistics.
        """
        if self.__downloads_age is None or self.__downloads is None:
            try:
                self.__downloads = get_download_stats(self.packages)
                self.__downloads_age = time()
            except BiocDownloadsError:
                self.__downloads = None
                self.__downloads_age = None
            except Exception as e:
                print(f"Packages might be invalid: {self.packages}")
                raise e
            return self.__downloads

        age = time() - self.__downloads_age

        if age / (60 * 60) > 8:
            self.__downloads = get_download_stats(self.packages)
            self.__downloads_age = time()
            return self.__downloads

        return self.__downloads

    @property
    def github_issues(self) -> dict[str, tuple[Issue] | None]:
        """Get the GitHub issues of each package.

        Returns:
            dict[str, tuple[Issue] | None]: Dict of Issues by package.
        """
        if not self.__github_age or self.__github_issues is None:
            self.__github_issues = get_issues(self.packages)
            self.__github_age = time()
            return self.__github_issues

        return self.__github_issues

    def parse_input(
        self,
        user_input: str | list[str],
    ) -> None:
        """Parse the user input.

        Args:
            user_input (str): The user input.
        """
        if isinstance(user_input, str):
            input_list = user_input.strip().split(" ")

            valid, invalid = [], []

            for package in input_list:
                if not package:
                    continue

                if package not in self.valid_paks:
                    invalid.append(package)
                else:
                    valid.append(package)

            if invalid:
                sep = ", " if (n_inv := len(invalid)) > 2 else ""
                message = (
                    ", ".join(invalid[:-2]) + sep + " and ".join(invalid[-2:])
                    if n_inv >= 2
                    else invalid[0]
                )

                st.warning(
                    f"{message} {'are' if n_inv > 1 else 'is'} not "
                    + f"{'a ' if n_inv == 1 else ''}valid Bioconductor package"
                    + f"{'s' if n_inv > 1 else ''}."
                )
        else:
            valid = user_input

        self.update_packages(valid)


def aggrid_interactive_table(status_df: pd.DataFrame) -> AgGridReturn:
    """Create an st-aggrid interactive table based on a dataframe.

    Args:
        status_df (pd.DataFrame]): Source dataframe

    Returns:
        AgGridReturn: The selected row
    """
    options = GridOptionsBuilder.from_dataframe(
        status_df, enableRowGroup=True, enableValue=True, enablePivot=True
    )

    options.configure_side_bar()

    options.configure_selection("single")
    selection = AgGrid(
        status_df,
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        gridOptions=options.build(),
        theme="streamlit",
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
    )

    return selection


def chunker(seq, size):
    """Transform ['a', 'b' ,'c'] into ['a', 'b'], ['c'] if size is 2."""
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def run_dash():
    """Generate the dashboard."""
    st.set_page_config(
        page_title="Package Status Dashboard",
    )
    st.title("Package Status Dashboard")
    st.write(
        """
    ### A dashboard for monitoring Bioconductor packages.
    """
    )

    # try to get a list of packages from Bioc
    with st.spinner("Getting the list of packages."):
        if "pak_list" in st.session_state:
            pak_list = st.session_state["pak_list"]
        else:
            try:
                pak_list = get_package_list()
            except Exception:
                pak_list = None

    if pak_list is not None:
        package_input = st.multiselect(
            label="Type in some Bioconductor packages.",
            options=list(pak_list.Name),
        )
        st.session_state["pak_list"] = pak_list
    else:
        package_input = st.text_input(
            label="Type in some Bioconductor packages separated by \
                spaces (e.g, BiocCheck BiocGenerics S4Vectors)."
        )

    if "data" not in st.session_state:
        with st.spinner("Scraping Bioconductor (this can take ~10 seconds)."):
            data = DashData()
            st.session_state["data"] = data
    else:
        data: DashData = st.session_state["data"]

    # assert isinstance(data, DashData)

    data.parse_input(package_input)

    if (age := (time() - data.soup_age) / (3600)) > 8:
        st.warning(
            "The scraped data are more than 8 hours old,"
            + " consider refreshing the page."
        )
    else:
        st.info(
            f"These data are about {round(age)}"
            + f" hour{'' if round(age) == 1 else 's'} old."
        )

    status_tab, download_tab, gh_tab = st.tabs(
        ["Bioc Build Status", "Downloads", "GitHub Issues"]
    )

    with status_tab:
        with st.spinner("Updating build status."):
            status_data = data.status_df

        for names in chunker(list(set(data.packages.Name)), 20):
            status_fig = (
                alt.Chart(status_data[status_data.Name.isin(names)])  # type: ignore
                .mark_square(  # type: ignore
                    size=500  # type: ignore
                )
                .encode(
                    x="Name",
                    y=alt.Y("Release:N", sort=("release", "devel")),  # type: ignore
                    color=alt.Color(
                        "Log Level",  # type: ignore
                        sort=["OK", "WARNINGS", "ERROR", "TIMEOUT"],  # type: ignore
                        scale=alt.Scale(  # type: ignore
                            domain=[
                                "OK",
                                "WARNINGS",
                                "ERROR",
                                "TIMEOUT",
                            ],  # type: ignore
                            range=["green", "orange", "red", "purple"],  # type: ignore
                        ),
                    ),
                )
                .configure_axis(labelFontSize=18)
                .configure_legend(orient="bottom")
                .properties(height=350)
            )

            st.altair_chart(status_fig, use_container_width=True)

        st.write(
            "Click on a row to view the message details.",
            " If there is no table  below, press `r`.",
        )
        selection = aggrid_interactive_table(
            status_df=status_data.sort_values(["Name"])
        )

        if selection.selected_rows:
            (
                _,
                name,  # package name
                pak_type,  # package type
                release,  # "release" or "devel"
                _,
                _,
                level,  # "OK", "WARNINGS", "TIMEOUT", "ERROR" or "NOT FOUND"
                stage,  # "INSTALL", "BUILD", "CHECK" or BUILD BIN"
                count,  # Number of warnings
                *messages,  # Warning messages
            ) = selection.selected_rows[0].values()
            if level == "OK":
                st.write(f"### No problems in the *{release}* build of **{name}**.")
            elif level == "NOT FOUND":
                st.write(f"### **{name}** was not found in Bioconductor.")
            else:
                # change warnings to warning if message count is smaller than 2
                level = (
                    "warning" if (int(count) < 2 and "W" in level) else level.lower()
                )
                url = build_urls(
                    package=name, type=pak_type, release=(r := release == "release"),
                    devel=not r
                )
                st.write(f"### {name} had {count} {level} during {stage}.\n")
                st.link_button(label="Build Results Link", url=url[0])

                for i, message in enumerate(messages):
                    if not message:
                        continue
                    st.write(f"**{level.capitalize().strip('s')} {i+1}**")
                    st.code(message, language="r")

    with download_tab:
        with st.spinner("Updating download stats."):
            dl_data = data.downloads

        if dl_data is None:
            st.warning(
                """
                    Download data could not be retrived.\n
                    The Bioconductor download stats are probably unavilable.\n
                    Please try again later.
                """
            )
            if st.button("Try Again"):
                with st.spinner("Updating download stats."):
                    dl_data = data.downloads

                if dl_data:
                    st.info("Success, reload the page.")
        else:
            include = st.multiselect(
                label="Choose which packages to include.",
                options=dl_data.Name.unique(),  # type: ignore
                default=dl_data.Name.unique(),  # type: ignore
            )
            dates = st.slider(
                label="Select a date range of interest.",
                min_value=(min_v := min(dl_data.Date).date()),  # type: ignore
                max_value=(max_v := max(dl_data.Date).date()),  # type: ignore
                value=(min_v, max_v),
                format="MMM YYYY",
            )
            log = st.checkbox("Log scale")
            ips = st.checkbox("Distinct IPs")

            min_date, max_date = [date(x.year, x.month, 1) for x in dates]

            # dummy index of True
            true_index = np.ones_like(dl_data.Downloads) == 1

            pack_index = (
                dl_data.Name.isin(include) if include else true_index  # type: ignore
            )
            date_index = (dl_data.Date.dt.date >= min_date) & (
                dl_data.Date.dt.date <= max_date
            )

            index = pack_index * date_index

            dl_fig = (
                alt.Chart(dl_data[index])
                .mark_line()
                .encode(
                    x="yearmonth(Date)",
                    y=alt.Y(
                        "Downloads" if not ips else "Distinct IPs",  # type: ignore
                        scale=alt.Scale(
                            type="symlog" if log else "linear"  # type: ignore
                        ),
                    ),
                    color="Name",
                )
            ).properties(height=400)

            st.altair_chart(dl_fig, use_container_width=True)

            st.download_button("Get data ", dl_data.to_csv(), "dl_data.csv")

    with gh_tab:
        with st.spinner("Updating GitHub data."):
            issue_data = data.github_issues

        not_found = [key for key, value in issue_data.items() if value is None]

        if not_found:
            # st.write(not_found)
            missing_str = (
                " and ".join("*" + x + "*" for x in not_found)
                if len(not_found) == 2
                else ", ".join(not_found)
            )
            st.write(
                "Could not find a repo link for: ",
                missing_str,
                ". ",
                "Consider pushing a bug report URL to Bioconductor.",
            )

        issue_plot_data = {k: len(v) for k, v in issue_data.items() if v is not None}

        issue_plot_data = pd.DataFrame(
            {"Name": issue_plot_data.keys(), "Issue Count": issue_plot_data.values()}
        ).set_index("Name")

        issue_fig = px.bar(
            issue_plot_data,
            y="Issue Count",
            labels={"Name": ""},
            template="plotly_dark",
        )
        issue_fig.update_xaxes(tickangle=-90, ticks="outside")

        st.write(
            "**Click** on the plot below to see issues of intest.",
            " If it is missing, press `r`.",
        )
        with st.container():
            selected = plotly_events(issue_fig)

        if selected:
            # st.write(selected)

            selected_name = selected[0]["x"]
            selected_issues = issue_data[selected_name]

            # st.write(selected_issues)

            if selected_issues:
                for i, issue in enumerate(selected_issues):
                    st.write(
                        f"**Issue {i+1}**: [{issue.title}]({issue.html_url})",
                        f" (#{issue.number})",
                    )

                    with st.expander("Show issue"):
                        st.markdown(issue.body.strip("\r"))

            else:
                st.write(f"{selected_name} has no issues!")

        # st.bar_chart(issue_plot_data)


# %%


if __name__ == "__main__":
    # run_dash()
    data = DashData()
