{{ config(materialized='table', engine='MergeTree()', order_by='report_id') }}

{#-
  One row per report_id: the 9 measures pivoted from bronze.rik_report_elements (long format).

  * Only allowlisted statement tables are read. When an element appears in several of them, the value of the
    highest-priority table (lowest number) wins: argMinIf(value, priority, condition).
  * Never read: consolidated / Konsolideerimata, Tulemiaruanne (non-profits), Likvideerimise*, Lõppbilanss,
    the cash-flow table and the PDF placeholder rows (empty elemendi_nimetus).
  * Values are cast with toDecimal64OrNull: missing or unparsable = NULL, never 0.
  * employee_expense is stored positive: abs(LaborExpense) from 'Lisa: Tööjõukulud'; if that is missing,
    abs(-1 * EmployeeExpense) from the income statement (filed with a minus sign). LaborExpense is filed
    negative in some reports, hence abs() on both. All other measures are kept as filed.
-#}

with elements as (
    select
        report_id,
        elemendi_nimetus as element,
        toDecimal64OrNull(trimBoth(vaartus), 2) as value,
        multiIf(
            tabel = 'Kasumiaruanne skeem 1', 1,
            tabel = 'Kasumiaruanne skeem 2', 2,
            NULL) as income_priority,
        multiIf(
            tabel = 'Bilanss', 1,
            tabel = 'Bilanss, detailne', 2,
            tabel = 'Bilanss, kasvava likviidsuse järjekorras', 3,
            NULL) as balance_priority,
        tabel = 'Lisa: Tööjõukulud' as is_labour_note
    from {{ source('bronze', 'rik_report_elements') }} final
    where elemendi_nimetus != ''
      and tabel in (
          'Kasumiaruanne skeem 1', 'Kasumiaruanne skeem 2',
          'Bilanss', 'Bilanss, detailne', 'Bilanss, kasvava likviidsuse järjekorras',
          'Lisa: Tööjõukulud')
),

pivoted as (
    select
        report_id,
        -- income statement
        argMinIf(value, income_priority, element = 'Revenue' and income_priority is not null) as revenue,
        argMinIf(value, income_priority, element = 'TotalProfitLoss' and income_priority is not null) as operating_profit,
        argMinIf(value, income_priority, element = 'EmployeeExpense' and income_priority is not null) as employee_expense_is,
        -- balance sheet
        argMinIf(value, balance_priority, element = 'Assets' and balance_priority is not null) as assets,
        argMinIf(value, balance_priority, element = 'Equity' and balance_priority is not null) as equity,
        argMinIf(value, balance_priority, element = 'CashAndCashEquivalents' and balance_priority is not null) as cash,
        argMinIf(value, balance_priority, element = 'CurrentAssets' and balance_priority is not null) as current_assets,
        argMinIf(value, balance_priority, element = 'CurrentLiabilities' and balance_priority is not null) as current_liabilities,
        -- labour note (one table, so no priority)
        anyIf(value, element = 'AverageNumberOfEmployeesInFullTimeEquivalentUnits' and is_labour_note) as avg_fte_raw,
        anyIf(value, element = 'LaborExpense' and is_labour_note) as labor_expense
    from elements
    group by report_id
)

select
    report_id,
    cast(revenue as Nullable(Decimal(18, 2))) as revenue,
    cast(operating_profit as Nullable(Decimal(18, 2))) as operating_profit,
    cast(assets as Nullable(Decimal(18, 2))) as assets,
    cast(equity as Nullable(Decimal(18, 2))) as equity,
    cast(cash as Nullable(Decimal(18, 2))) as cash,
    cast(current_assets as Nullable(Decimal(18, 2))) as current_assets,
    cast(current_liabilities as Nullable(Decimal(18, 2))) as current_liabilities,
    cast(coalesce(abs(labor_expense), abs(-1 * employee_expense_is)) as Nullable(Decimal(18, 2))) as employee_expense,
    cast(avg_fte_raw as Nullable(Decimal(10, 2))) as avg_fte,
    toUInt8(
        revenue is not null or operating_profit is not null or assets is not null or equity is not null
        or cash is not null or current_assets is not null or current_liabilities is not null
        or coalesce(labor_expense, employee_expense_is) is not null or avg_fte_raw is not null
    ) as has_financials
from pivoted
