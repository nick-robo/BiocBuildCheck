"""Runs a dashboard to visualise the build status of Bioconductor packages
"""

from datetime import date
from pickle import load
from typing import Iterable, Optional
from warnings import simplefilter

import altair as alt
import numpy as np
import pandas as pd
import plotly_express as px
import streamlit as st
import streamlit_analytics
from github.Issue import Issue
from st_aggrid import (AgGrid, AgGridReturn, ColumnsAutoSizeMode,
                       GridOptionsBuilder)
from st_aggrid.shared import GridUpdateMode
from streamlit_plotly_events import plotly_events

from check import get_download_stats, get_info, get_issues, get_package_status

# ignore fufturewarning thrown by AgGrid
simplefilter("ignore", FutureWarning)


def aggrid_interactive_table(status_df: pd.DataFrame) -> AgGridReturn:
    """Creates an st-aggrid interactive table based on a dataframe.

    Args:
        df (pd.DataFrame]): Source dataframe

    Returns:
        dict: The selected row
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


@st.cache(ttl=3600)
def get_build_data(packages: Optional[Iterable[str]] = None) -> pd.DataFrame:
    """Gets the build data necessary for the dashboard.

    Args:
        packages (Optional[Iterable[str]], optional): List of packages of interest. Default is None.

    Returns:
        pd.DataFrame: The dashboard data.
    """

    status_df = get_package_status(packages=packages, devel=True)
    get_info(status_df)

    return status_df


@st.cache(ttl=10*3600)
def get_dl_data(status_df: pd.DataFrame) -> pd.DataFrame:
    """Gets the download data necessary for the dashboard.

    Args:
        status_df (pd.DataFrame): DataFrame of statuses, generated by `get_build_data`.

    Returns:
        pd.DataFrame: DF of download stats.
    """
    return get_download_stats(status_df=status_df)


def parse_input(user_input: str) -> list[str]:
    """Parses the user input.

    Args:
        user_input (str): The user input.

    Returns:
        list[str]: Parsed input.
    """

    input_list = user_input.strip().split(" ")

    return [x for x in input_list if x]


@st.cache(ttl=10*3600)
def get_issue_data(status_df: pd.DataFrame, dev: bool = False) -> dict[str, Optional[list[Issue]]]:
    """Gets the issue data necessary for the dashboard.

    Args:
        status_df (pd.DataFrame): DataFrame of statuses, generated by `get_build_data`.

    Returns:
        pd.DataFrame: DF of download stats.
    """
    if dev:
        with open("issues.pkl", "rb") as f:
            issues = load(f)
    else:
        issues = get_issues(status_df)

    return issues


def run_dash():
    """Generates the dashboard.
    """

    st.title("Package Status Dashboard")
    st.write("""
    ### A little dashboard for monitoring your Bioconductor packages of interest.
    """)

    package_input = st.text_input(
        label="Type in some Bioconductor packages separated by \
               spaces (e.g, BiocCheck BiocGenerics S4Vectors).")

    packages = parse_input(package_input) if package_input else None

    # get the data
    package_data = get_build_data(packages=packages)

    status_tab, download_tab, gh_tab = st.tabs(
        ["Bioc Build Status", "Downloads", "GitHub Issues"])

    with status_tab:

        st.write("### Bioconductor Build Status")
        status_fig = alt.Chart(package_data).mark_square(  # type: ignore
            size=500  # type: ignore
        ).encode(
            x="Name",
            y=alt.Y("Release:N", sort=("release", "devel")),  # type: ignore
            color=alt.Color(
                "Log Level",  # type: ignore
                sort=["OK", "WARNINGS", "ERROR",
                      "NOT FOUND", "TIMEOUT"],  # type: ignore
                scale=alt.Scale(  # type: ignore
                    domain=["OK", "WARNINGS", "ERROR",
                            "NOT FOUND", "TIMEOUT"],  # type: ignore
                    range=["green", "orange", "red", "purple", "blue"])  # type: ignore
            )
        ).configure_axis(
            labelFontSize=18
        ).properties(
            height=250
        )

        st.altair_chart(status_fig, use_container_width=True)

        st.write("Click on a row to view the message details.")
        selection = aggrid_interactive_table(
            status_df=package_data.sort_values(["Name"]))

        if selection.selected_rows:
            _, name, release, log_level, stage, count, * \
                messages = selection.selected_rows[0].values()
            if log_level == "OK":
                st.write(
                    f"### There were no problems in the *{release}* build of **{name}**.")
            elif log_level == "NOT FOUND":
                st.write(
                    f"### **{name}** was not found in the list of Bioconductor packages.")
            else:
                # change warnings to warning if message count is smaller than 2
                log_level = "warning" if (
                    int(count) < 2 and "W" in log_level) else log_level.lower()
                st.write(f"### {name} had {count} {log_level} during {stage}.")

                for i, message in enumerate(messages):
                    if not message:
                        continue
                    st.write(f"**{log_level.capitalize().strip('s')} {i+1}**")
                    st.code(message, language="r")

    with download_tab:
        dl_data = get_dl_data(package_data)

        st.write("### Filters")

        include = st.multiselect(
            label="Choose which packages to include.",
            options=dl_data.Name.unique(),  # type: ignore
            default=dl_data.Name.unique()  # type: ignore
        )
        dates = st.slider(
            label="Select a date range of interest.",
            min_value=(min_v := min(dl_data.Date).date()),
            max_value=(max_v := max(dl_data.Date).date()),
            value=(min_v, max_v),
            format="MMM YYYY",
        )
        log = st.checkbox("Log scale")

        min_date, max_date = [date(x.year, x.month, 1) for x in dates]

        # dummy index of True
        true_index = np.ones_like(dl_data.Downloads) == 1

        pack_index = dl_data.Name.isin(  # type: ignore
            include) if include else true_index
        date_index = (dl_data.Date.dt.date >= min_date) & (
            dl_data.Date.dt.date <= max_date)

        index = pack_index * date_index

        dl_fig = (
            alt.Chart(
                dl_data[index]
            )
            .mark_line()
            .encode(
                x="yearmonth(Date)",
                y=alt.Y("Downloads", scale=alt.Scale(  # type: ignore
                    type="symlog" if log else "linear")),  # type: ignore
                color="Name"
            )
        ).properties(
            height=400
        )

        st.write("### Plot")

        st.altair_chart(dl_fig, use_container_width=True)

    with gh_tab:
        issue_data = get_issue_data(package_data, dev=True)
        st.write("### GitHub Issues")

        not_found = [key for key, value in issue_data.items() if value is None]

        if not_found:
            # st.write(not_found)
            st.write(
                "Could not find a repo link for: ",
                " and ".join("*" + x + "*" for x in not_found) if len(not_found) == 2 else ", ".join(
                    not_found), ". ", "Consider pushing a bug report URL to Bioconductor."
            )

        issue_plot_data = {k: len(v)
                           for k, v in issue_data.items() if v != None}

        issue_plot_data = pd.DataFrame(
            {
                "Name": issue_plot_data.keys(),
                "Issue Count": issue_plot_data.values()
            }
        ).set_index("Name")

        issue_fig = px.bar(issue_plot_data, y="Issue Count",
                           template="plotly_dark")

        st.write("**Click** on the plot below to see issues of intest.")
        selected = plotly_events(issue_fig)

        if selected:
            # st.write(selected)

            selected_name = selected[0]["x"]
            selected_issues = issue_data[selected_name]

            # st.write(selected_issues)

            if selected_issues:

                for i, issue in enumerate(selected_issues):
                    st.write(
                        f"**Issue {i+1}**: [{issue.title}]({issue.html_url}) (#{issue.number})")

                    with st.expander("Show issue"):
                        st.markdown(issue.body.strip("\r"))

            else:
                st.write(f"{selected_name} has no issues!")

        # st.bar_chart(issue_plot_data)


if __name__ == "__main__":
    with streamlit_analytics.track():
        run_dash()
