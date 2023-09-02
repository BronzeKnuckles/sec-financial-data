import os, json, pandas as pd


files = ["3M COMPANY.json"]

# os.listdir("./data/companyfacts/3M Co")
"""facts = [
    "AccountsPayableAndOtherAccruedLiabilitiesCurrent",
    "AccruedLiabilitiesCurrent",
    "AdditionalPaidInCapital",
    "AdjustmentsToAdditionalPaidInCapitalOther",
    "Assets",
    "AssetsCurrent",
    "AssetsHeldInTrustNoncurrent",
    "Cash",
    "CashAndCashEquivalentsAtCarryingValue",
    "CashAndCashEquivalentsPeriodIncreaseDecrease",
    "CashEquivalentsAtCarryingValue",
    "CashFDICInsuredAmount",
    "DerivativeLiabilitiesNoncurrent",
    "EarningsPerShareBasicAndDiluted",
    "FairValueAdjustmentOfWarrants",
    "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisLiabilityGainLossIncludedInEarnings",
    "GainLossOnSaleOfDerivatives",
    "IncreaseDecreaseInAccountsPayableAndOtherOperatingLiabilities",
    "IncreaseDecreaseInAccruedLiabilities",
    "IncreaseDecreaseInPrepaidExpense",
    "InterestAndDividendIncomeOperating",
    "Liabilities",
    "LiabilitiesAndStockholdersEquity",
    "LiabilitiesCurrent",
    "LiquidationBasisOfAccountingAccruedCostsToDisposeOfAssetsAndLiabilities",
    "MinimumNetWorthRequiredForCompliance",
    "NetCashProvidedByUsedInFinancingActivities",
    "NetCashProvidedByUsedInInvestingActivities",
    "NetCashProvidedByUsedInOperatingActivities",
    "NetIncomeLoss",
    "NotesPayableRelatedPartiesClassifiedCurrent",
    "OperatingCostsAndExpenses",
    "OperatingIncomeLoss",
    "PaymentsForUnderwritingExpense",
    "PaymentsOfStockIssuanceCosts",
    "PaymentsToAcquireRestrictedInvestments",
    "PreferredStockParOrStatedValuePerShare",
    "PreferredStockSharesAuthorized",
    "PreferredStockSharesIssued",
    "PreferredStockSharesOutstanding",
    "PreferredStockValue",
    "PrepaidExpenseCurrent",
    "ProceedsFromIssuanceInitialPublicOffering",
    "ProceedsFromIssuanceOfCommonStock",
    "ProceedsFromIssuanceOfPrivatePlacement",
    "ProceedsFromIssuanceOfUnsecuredDebt",
    "RelatedPartyTransactionExpensesFromTransactionsWithRelatedParty",
    "RepaymentsOfRelatedPartyDebt",
    "RetainedEarningsAccumulatedDeficit",
    "StockholdersEquity",
    "StockIssuedDuringPeriodValueIssuedForServices",
    "StockIssuedDuringPeriodValueNewIssues",
    "TemporaryEquityAccretionToRedemptionValueAdjustment",
    "TemporaryEquityCarryingAmountAttributableToParent",
    "TemporaryEquityRedemptionPricePerShare",
    "UnrecognizedTaxBenefits",
    "UnrecognizedTaxBenefitsIncomeTaxPenaltiesAndInterestAccrued",
    "WeightedAverageNumberOfShareOutstandingBasicAndDiluted",
    "AccountsPayableCurrent",
    "AccountsPayableRelatedPartiesCurrent",
    "IncreaseDecreaseInAccountsPayable",
    "ProceedsFromOtherEquity",
    "ProceedsFromRelatedPartyDebt",
    "RepaymentsOfDebt",
    "TemporaryEquityAccretionToRedemptionValue",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    "StockIssuedDuringPeriodValueOther",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",
    "BusinessCombinationRecognizedIdentifiableAssetsAcquiredAndLiabilitiesAssumedCurrentAssetsReceivables",
    "ClassOfWarrantOrRightExercisePriceOfWarrantsOrRights1",
    "InvestmentIncomeInterest",
    "ClassOfWarrantOrRightNumberOfSecuritiesCalledByWarrantsOrRights",
    "DerivativeInstrumentsNotDesignatedAsHedgingInstrumentsGainLossNet",
]"""
columns = [
    "CompanyName",
    "cik",
    "fact",
    "units" "end",
    "val",
    "accn",
    "fy",
    "fp",
    "form",
    "filed",
]


table = pd.DataFrame(columns=columns)

for company in files:
    data = open(f"./data/companyfacts/{company}")
    df = json.load(data)
    company_name = df["entityName"]
    cik = df["cik"]
    facts = df["facts"]["us-gaap"].keys()
    for fact in facts:
        dynamic_col = str(list(df["facts"]["us-gaap"][fact]["units"].keys())[0])
        for i in range(len(df["facts"]["us-gaap"][fact]["units"][dynamic_col])):
            # df["facts"]["us-gaap"]["Cash"]["units"]["USD"][i]

            row = {
                "CompanyName": company_name,
                "cik": cik,
                "fact": fact,
                "units": df["facts"]["us-gaap"][fact]["units"][dynamic_col],
                "end": df["facts"]["us-gaap"][fact]["units"][dynamic_col][i]["end"],
                "val": df["facts"]["us-gaap"][fact]["units"][dynamic_col][i]["val"],
                "accn": df["facts"]["us-gaap"][fact]["units"][dynamic_col][i]["accn"],
                "fy": df["facts"]["us-gaap"][fact]["units"][dynamic_col][i]["fy"],
                "fp": df["facts"]["us-gaap"][fact]["units"][dynamic_col][i]["fp"],
                "form": df["facts"]["us-gaap"][fact]["units"][dynamic_col][i]["form"],
                "filed": df["facts"]["us-gaap"][fact]["units"][dynamic_col][i]["filed"],
            }
            row = pd.Series(row)

            table = pd.concat([table, row], ignore_index=True)


table.to_csv("./table.csv")
