from unittest.mock import patch

import pandas as pd
import pytest

from dags.etl_modules import vci_provider


@pytest.mark.unit
class TestFinancialStatementSelection:
    @patch("dags.etl_modules.vci_provider._graphql")
    @patch("dags.etl_modules.vci_provider._company_type_code")
    @patch("dags.etl_modules.vci_provider._financial_ratio_metadata")
    def test_income_statement_selects_income_fields_only(
        self, mock_metadata, mock_company_type, mock_graphql
    ):
        mock_company_type.return_value = "CT"
        mock_metadata.return_value = pd.DataFrame(
            [
                {
                    "field_name": "is_net_sales",
                    "en_name": "Net Sales",
                    "name": "Doanh thu thuần",
                    "type": "Chỉ tiêu kết quả kinh doanh",
                    "com_type_code": "CT",
                    "order": 1,
                },
                {
                    "field_name": "bs_total_asset",
                    "en_name": "Total Asset",
                    "name": "Tổng tài sản",
                    "type": "Chỉ tiêu cân đối kế toán",
                    "com_type_code": "CT",
                    "order": 2,
                },
            ]
        )
        mock_graphql.return_value = {
            "CompanyFinancialRatio": {
                "ratio": [
                    {
                        "ticker": "HPG",
                        "yearReport": 2024,
                        "lengthReport": 4,
                        "updateDate": "2024-12-31",
                        "is_net_sales": 1000,
                        "bs_total_asset": 5000,
                    }
                ]
            }
        }

        df = vci_provider.fetch_income_statement("HPG", period="Q")

        assert "Net Sales" in df.columns
        assert "Total Asset" not in df.columns

    @patch("dags.etl_modules.vci_provider._graphql")
    @patch("dags.etl_modules.vci_provider._company_type_code")
    @patch("dags.etl_modules.vci_provider._financial_ratio_metadata")
    def test_balance_sheet_selects_balance_fields_only(
        self, mock_metadata, mock_company_type, mock_graphql
    ):
        mock_company_type.return_value = "CT"
        mock_metadata.return_value = pd.DataFrame(
            [
                {
                    "field_name": "is_net_sales",
                    "en_name": "Net Sales",
                    "name": "Doanh thu thuần",
                    "type": "Chỉ tiêu kết quả kinh doanh",
                    "com_type_code": "CT",
                    "order": 1,
                },
                {
                    "field_name": "bs_total_asset",
                    "en_name": "Total Asset",
                    "name": "Tổng tài sản",
                    "type": "Chỉ tiêu cân đối kế toán",
                    "com_type_code": "CT",
                    "order": 2,
                },
            ]
        )
        mock_graphql.return_value = {
            "CompanyFinancialRatio": {
                "ratio": [
                    {
                        "ticker": "HPG",
                        "yearReport": 2024,
                        "lengthReport": 4,
                        "updateDate": "2024-12-31",
                        "is_net_sales": 1000,
                        "bs_total_asset": 5000,
                    }
                ]
            }
        }

        df = vci_provider.fetch_balance_sheet("HPG", period="Q")

        assert "Total Asset" in df.columns
        assert "Net Sales" not in df.columns


@pytest.mark.unit
class TestIndustryMetadataNormalization:
    @patch("dags.etl_modules.vci_provider._graphql")
    def test_industry_metadata_derives_icb_code_from_icb_code4(self, mock_graphql):
        mock_graphql.return_value = {
            "CompaniesListingInfo": [
                {
                    "ticker": "HPG",
                    "icbName2": "Materials",
                    "icbName3": "Steel",
                    "icbCode4": "55101010",
                }
            ]
        }

        df = vci_provider.fetch_vn_industry_metadata()

        assert "icb_code" in df.columns
        assert df.loc[0, "icb_code"] == "55101010"
