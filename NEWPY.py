from collections import defaultdict
from datetime import date, datetime, timedelta
import calendar

from flask import render_template, request


# ============================================================
# EXCEL DATE CONSTANT
# ============================================================

EXCEL_EPOCH = date(1899, 12, 30)


# ============================================================
# EXCEL SERIAL -> PYTHON DATE
# ============================================================

def excel_to_date(value):

    if value is None:
        return None

    try:
        return EXCEL_EPOCH + timedelta(days=int(value))
    except (ValueError, TypeError, OverflowError):
        return None


# ============================================================
# PYTHON DATE -> EXCEL SERIAL
# ============================================================

def date_to_excel(value):

    return (value - EXCEL_EPOCH).days


# ============================================================
# CONVERT LEAVING DATE
#
# Your DateofLeavingRSAASIPL is String(255), so this function
# handles multiple possible formats.
# ============================================================

def parse_leaving_date(value):

    if value is None:
        return None

    # Already datetime
    if isinstance(value, datetime):
        return value.date()

    # Already date
    if isinstance(value, date):
        return value

    # Excel integer
    if isinstance(value, int):
        return excel_to_date(value)

    # String
    if isinstance(value, str):

        value = value.strip()

        if not value:
            return None

        # Excel number stored as string
        try:

            if value.isdigit():

                return excel_to_date(
                    int(value)
                )

        except Exception:

            pass

        possible_formats = [

            "%Y-%m-%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%d-%b-%Y",
            "%d %b %Y",
            "%d-%B-%Y",
            "%d %B %Y"

        ]

        for fmt in possible_formats:

            try:

                return datetime.strptime(
                    value,
                    fmt
                ).date()

            except ValueError:

                continue

    return None


# ============================================================
# OVERALL TIMESHEET SUMMARY
# ============================================================

@admin_bp.route(
    "/admin/timesheet/overalltimesheetsummary"
)
@login_required
@admin_required
def overalltimesheetsummary():

    # ========================================================
    # CURRENT DATE
    # ========================================================

    today = datetime.now()

    # ========================================================
    # DEFAULT = PREVIOUS MONTH
    # ========================================================

    if today.month == 1:

        default_month = 12
        default_year = today.year - 1

    else:

        default_month = today.month - 1
        default_year = today.year

    # ========================================================
    # MONTH
    # ========================================================

    try:

        selected_month = int(
            request.args.get(
                "month",
                default_month
            )
        )

    except (TypeError, ValueError):

        selected_month = default_month

    # ========================================================
    # YEAR
    # ========================================================

    try:

        selected_year = int(
            request.args.get(
                "year",
                default_year
            )
        )

    except (TypeError, ValueError):

        selected_year = default_year

    # ========================================================
    # SELECTED MONTH DATE RANGE
    # ========================================================

    first_day = date(
        selected_year,
        selected_month,
        1
    )

    last_day_number = calendar.monthrange(
        selected_year,
        selected_month
    )[1]

    last_day = date(
        selected_year,
        selected_month,
        last_day_number
    )

    # ========================================================
    # EXCEL SERIAL DATES
    # ========================================================

    start_excel = date_to_excel(
        first_day
    )

    end_excel = date_to_excel(
        last_day
    )

    # ========================================================
    # BUILD WEEKS
    #
    # Current definition:
    #
    # Week 1 = 1 - 7
    # Week 2 = 8 - 14
    # Week 3 = 15 - 21
    # Week 4 = 22 - 28
    # Week 5 = 29 - end
    #
    # IMPORTANT:
    # We still evaluate only Monday-Friday inside these ranges.
    # ========================================================

    week_ranges = []

    for start_day in range(
        1,
        last_day_number + 1,
        7
    ):

        end_day = min(
            start_day + 6,
            last_day_number
        )

        week_start = date(
            selected_year,
            selected_month,
            start_day
        )

        week_end = date(
            selected_year,
            selected_month,
            end_day
        )

        week_ranges.append({

            "number":
                len(week_ranges) + 1,

            "start":
                week_start.strftime(
                    "%Y-%m-%d"
                ),

            "end":
                week_end.strftime(
                    "%Y-%m-%d"
                ),

            "label":
                (
                    f"{week_start.strftime('%d %b')}"
                    f" - "
                    f"{week_end.strftime('%d %b')}"
                )

        })

    # ========================================================
    # ACTIVE EMPLOYEES
    #
    # IMPORTANT:
    # We start from EmpWD because we need employees who have
    # ZERO timesheet records as well.
    # ========================================================

    active_employees = (

        EmpWD.query

        .filter(
            EmpWD.Status == "Active"
        )

        .order_by(
            EmpWD.Team,
            EmpWD.EName
        )

        .all()

    )

    # ========================================================
    # EMPLOYEE DATA
    # ========================================================

    employees = {}

    for emp in active_employees:

        # ----------------------------------------------------
        # DOJ
        # ----------------------------------------------------

        doj = emp.DOJRS

        if isinstance(doj, datetime):

            doj = doj.date()

        # ----------------------------------------------------
        # LEAVING DATE
        # ----------------------------------------------------

        leaving_date = parse_leaving_date(
            emp.DateofLeavingRSAASIPL
        )

        employees[emp.EMPID] = {

            "emp_id":
                emp.EMPID,

            "name":
                emp.EName or "",

            "team":
                emp.Team or "Unknown",

            "doj":
                doj.strftime("%Y-%m-%d")
                if doj
                else None,

            "leaving_date":
                leaving_date.strftime("%Y-%m-%d")
                if leaving_date
                else None,

            "submitted":
                False,

            "missing_weeks":
                0,

            "eligible_weeks":
                0,

            "weeks":
                []

        }

    # ========================================================
    # TIMESHEET DATA
    #
    # We only need records in selected month.
    # ========================================================

    timesheet_rows = (

        TimesheetEntry.query

        .filter(
            TimesheetEntry.DateofEntry >= start_excel,
            TimesheetEntry.DateofEntry <= end_excel
        )

        .all()

    )

    # ========================================================
    # SUBMITTED DAYS
    #
    # submitted_days[EmpID][date] = number of submitted rows
    #
    # Multiple entries on same day are therefore supported.
    # ========================================================

    submitted_days = defaultdict(
        lambda: defaultdict(int)
    )

    for entry in timesheet_rows:

        # ----------------------------------------------------
        # Employee must be in active employee list
        # ----------------------------------------------------

        if entry.EmpID not in employees:

            continue

        # ----------------------------------------------------
        # Date
        # ----------------------------------------------------

        if entry.DateofEntry is None:

            continue

        entry_date = excel_to_date(
            entry.DateofEntry
        )

        if entry_date is None:

            continue

        # ----------------------------------------------------
        # Ignore Saturday / Sunday
        # ----------------------------------------------------

        if entry_date.weekday() >= 5:

            continue

        # ----------------------------------------------------
        # Status
        # ----------------------------------------------------

        if not entry.Status:

            continue

        if (
            entry.Status.strip().lower()
            != "submitted"
        ):

            continue

        # ----------------------------------------------------
        # Count submitted entry
        # ----------------------------------------------------

        submitted_days[
            entry.EmpID
        ][
            entry_date
        ] += 1

    # ========================================================
    # BUILD WEEK STATUS
    # ========================================================

    for emp_id, employee in employees.items():

        # ----------------------------------------------------
        # Employee DOJ
        # ----------------------------------------------------

        doj = None

        if employee["doj"]:

            doj = datetime.strptime(
                employee["doj"],
                "%Y-%m-%d"
            ).date()

        # ----------------------------------------------------
        # Leaving date
        # ----------------------------------------------------

        leaving_date = None

        if employee["leaving_date"]:

            leaving_date = datetime.strptime(
                employee["leaving_date"],
                "%Y-%m-%d"
            ).date()

        employee["weeks"] = []

        # ----------------------------------------------------
        # LOOP THROUGH WEEKS
        # ----------------------------------------------------

        for week in week_ranges:

            week_start = datetime.strptime(
                week["start"],
                "%Y-%m-%d"
            ).date()

            week_end = datetime.strptime(
                week["end"],
                "%Y-%m-%d"
            ).date()

            eligible_days = []

            submitted_working_days = 0

            current_day = week_start

            # =================================================
            # LOOP THROUGH DAYS
            # =================================================

            while current_day <= week_end:

                # ---------------------------------------------
                # Saturday / Sunday
                # ---------------------------------------------

                if current_day.weekday() >= 5:

                    current_day += timedelta(days=1)

                    continue

                # ---------------------------------------------
                # BEFORE DOJ
                # ---------------------------------------------

                if (
                    doj
                    and current_day < doj
                ):

                    current_day += timedelta(days=1)

                    continue

                # ---------------------------------------------
                # AFTER LEAVING DATE
                # ---------------------------------------------

                if (
                    leaving_date
                    and current_day > leaving_date
                ):

                    current_day += timedelta(days=1)

                    continue

                # ---------------------------------------------
                # ELIGIBLE WORKING DAY
                # ---------------------------------------------

                eligible_days.append(
                    current_day
                )

                # ---------------------------------------------
                # SUBMITTED?
                # ---------------------------------------------

                if (
                    submitted_days[
                        emp_id
                    ].get(
                        current_day,
                        0
                    ) > 0
                ):

                    submitted_working_days += 1

                current_day += timedelta(days=1)

            # =================================================
            # WEEK RESULT
            # =================================================

            total_eligible_days = len(
                eligible_days
            )

            # -------------------------------------------------
            # NOT EMPLOYED DURING WEEK
            # -------------------------------------------------

            if total_eligible_days == 0:

                status = "not_eligible"

            # -------------------------------------------------
            # ALL ELIGIBLE DAYS SUBMITTED
            # -------------------------------------------------

            elif (
                submitted_working_days
                == total_eligible_days
            ):

                status = "submitted"

                employee[
                    "eligible_weeks"
                ] += 1

            # -------------------------------------------------
            # SOME DAYS MISSING
            # -------------------------------------------------

            else:

                status = "missing"

                employee[
                    "eligible_weeks"
                ] += 1

                employee[
                    "missing_weeks"
                ] += 1

            # -------------------------------------------------
            # SAVE WEEK
            # -------------------------------------------------

            employee["weeks"].append({

                "number":
                    week["number"],

                "start":
                    week["start"],

                "end":
                    week["end"],

                "label":
                    week["label"],

                "status":
                    status,

                "eligible_days":
                    total_eligible_days,

                "submitted_days":
                    submitted_working_days

            })

        # =====================================================
        # EMPLOYEE MONTH STATUS
        # =====================================================

        eligible_weeks = [

            week

            for week in employee["weeks"]

            if week["status"]
            != "not_eligible"

        ]

        missing_weeks = [

            week

            for week in eligible_weeks

            if week["status"]
            == "missing"

        ]

        # -----------------------------------------------------
        # FULLY SUBMITTED
        # -----------------------------------------------------

        employee["submitted"] = (

            len(eligible_weeks) > 0

            and

            len(missing_weeks) == 0

        )

    # ========================================================
    # TEAM LIST
    # ========================================================

    teams = sorted({

        employee["team"]

        for employee in employees.values()

        if employee["team"]

    })

    # ========================================================
    # OVERALL COUNTS
    #
    # These are sent to JS so they can update dynamically
    # when Team / Status filters are changed.
    # ========================================================

    total_active = len(
        employees
    )

    total_submitted = sum(

        1

        for employee
        in employees.values()

        if employee["submitted"]

    )

    total_non_submitted = (

        total_active
        - total_submitted

    )

    # ========================================================
    # RETURN PAGE
    # ========================================================

    return render_template(

        "overalltimesheetsummary.html",

        employees=list(
            employees.values()
        ),

        teams=teams,

        week_ranges=week_ranges,

        selected_month=
            selected_month,

        selected_year=
            selected_year,

        current_year=
            today.year,

        total_active=
            total_active,

        total_submitted=
            total_submitted,

        total_non_submitted=
            total_non_submitted

    )