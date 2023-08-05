from fastapi import APIRouter
from fastapi import Depends, HTTPException
from fastapi import HTTPException
from fastapi import Query, Path, HTTPException
from google.cloud import exceptions
from fastapi import Depends, HTTPException
from ..api import get_current_user, db

router = APIRouter()


@router.get(
    "/{protocol_name}/asset",
    tags=["Messari - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Asset Info",
            "content": {
                "application/json": {
                    "example": {
                        "data": {
                            "symbol": "COMP",
                            "_internal_temp_agora_id": "b4a4024c-14c1-4e90-b092-b27cb58b7521",
                            "serial_id": 350,
                            "name": "Compound",
                            "id": "157f4fe3-6046-4b6d-bceb-a2af8ca021b5",
                            "contract_addresses": [
                                {
                                    "contract_address": "0x8505b9d2254a7ae468c0e9dd10ccea3a837aef5c",
                                    "platform": "polygon-pos",
                                },
                                {
                                    "contract_address": "0x354a6da3fcde098f8389cad84b0182725c6c91de",
                                    "platform": "arbitrum-one",
                                },
                                {
                                    "contract_address": "c00e94cb662c3520282e6f5717214004a7f26888.factory.bridge.near",
                                    "platform": "near-protocol",
                                },
                                {
                                    "contract_address": "0x32137b9275ea35162812883582623cd6f6950958",
                                    "platform": "harmony-shard-0",
                                },
                                {
                                    "contract_address": "0x66bc411714e16b6f0c68be12bd9c666cc4576063",
                                    "platform": "energi",
                                },
                                {
                                    "contract_address": "0x52ce071bd9b1c4b00a0b92d298c512478cad67e8",
                                    "platform": "binance-smart-chain",
                                },
                                {
                                    "contract_address": "0xc3048e19e76cb9a3aa9d77d8c03c29fc906e2437",
                                    "platform": "avalanche",
                                },
                                {
                                    "contract_address": "0xc00e94cb662c3520282e6f5717214004a7f26888",
                                    "platform": "ethereum",
                                },
                                {
                                    "contract_address": "0x00dbd45af9f2ea406746f9025110297469e9d29efc60df8d88efb9b0179d6c2c",
                                    "platform": "sora",
                                },
                            ],
                            "slug": "compound",
                        },
                        "status": {
                            "elapsed": 1,
                            "timestamp": "2023-08-03T18:34:21.65422534Z",
                        },
                    }
                }
            },
        },
        204: {
            "description": "No content found.",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def asset(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the asset data from Messari.

    """

    try:
        ref = db.collection(f"{protocol_name}-messari").document(f"asset").get(field_paths=["data"]).to_dict()

        if ref is None:
            raise exceptions.NotFound("Collection or document not found")

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@router.get(
    "/{protocol_name}/asset-profile",
    tags=["Messari - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Messari Asset Profile",
            "content": {
                "application/json": {
                    "example": {
                        "data": {
                            "symbol": "COMP",
                            "_internal_temp_agora_id": "b4a4024c-14c1-4e90-b092-b27cb58b7521",
                            "profile": {
                                "general": {
                                    "overview": {
                                        "project_details": "Compound is a lending platform built on Ethereum that enables users to permissionlessly borrow or lend from a pool of assets. Rather than interest rates being set by individuals, they are determined algorithmically based on the proportion of assets lent out.",
                                        "tagline": "Decentralized money market protocol",
                                        "official_links": [
                                            {
                                                "name": "Website",
                                                "link": "https://compound.finance/",
                                            }
                                        ],
                                        "category": "Financial",
                                        "is_verified": False,
                                        "sector": "Lending",
                                        "tags": "DeFi",
                                    },
                                    "regulation": {
                                        "sfar_summary": None,
                                        "regulatory_details": None,
                                        "sfar_score": None,
                                    },
                                    "background": {
                                        "background_details": "Compound was started ...",
                                        "issuing_organizations": [],
                                    },
                                    "roadmap": [
                                        {
                                            "date": "2018-09-27T04:00:00Z",
                                            "details": None,
                                            "title": "Compound launches on mainnet",
                                            "type": None,
                                        }
                                    ],
                                },
                                "economics": {
                                    "consensus_and_emission": {
                                        "consensus": {
                                            "precise_consensus_mechanism": None,
                                            "is_victim_of_51_percent_attack": None,
                                            "block_reward": None,
                                            "consensus_details": "",
                                            "next_halving_date": None,
                                            "general_consensus_mechanism": "n/a",
                                            "targeted_block_time": None,
                                            "mining_algorithm": None,
                                        },
                                        "supply": {
                                            "max_supply": 10000000,
                                            "is_capped_supply": True,
                                            "precise_emission_type": "Fixed Supply",
                                            "supply_curve_details": "A total of 10 million COMP tokens ...",
                                            "general_emission_type": "Fixed Supply",
                                        },
                                    },
                                    "native_treasury": {
                                        "treasury_usage_details": None,
                                        "accounts": [],
                                    },
                                    "launch": {
                                        "general": {
                                            "launch_style": "Private Sale",
                                            "launch_details": "Tokens were initially distributed to shareholders of Compound Labs, Inc. who trialed token governance for a few months. Afterward, the distribution for users of the platform began, releasing tokens continuously for the next four years split equally between lenders and borrowers of various pools based on the proportion of total interest paid out. This process, known as liquidity mining, generated substantial attention around the DeFi community and increased the total value of assets locked from $100 million to over $600 million in the first week.",
                                        },
                                        "fundraising": {
                                            "sales_documents": [
                                                {
                                                    "name": "SEC Form D ",
                                                    "link": "https://www.sec.gov/Archives/edgar/data/1736957/000173695718000001/xslFormDX01/primary_doc.xml",
                                                }
                                            ],
                                            "sales_treasury_accounts": [],
                                            "projected_use_of_sales_proceeds": [],
                                            "sales_rounds": [
                                                {
                                                    "end_date": None,
                                                    "asset_collected": None,
                                                    "amount_collected_in_asset": None,
                                                    "is_kyc_required": None,
                                                    "title": "Seed Equity",
                                                    "type": "Private",
                                                    "native_tokens_allocated": None,
                                                    "price_per_token_in_asset": None,
                                                    "equivalent_price_per_token_in_usd": None,
                                                    "amount_collected_in_usd": 8200000,
                                                    "restricted_jurisdictions": None,
                                                    "details": None,
                                                    "start_date": None,
                                                },
                                            ],
                                            "treasury_policies": None,
                                        },
                                        "initial_distribution": {
                                            "genesis_block_date": None,
                                            "initial_supply_repartition": {
                                                "allocated_to_organization_or_founders_percentage": None,
                                                "allocated_to_investors_percentage": None,
                                                "allocated_to_premined_rewards_or_airdrops_percentage": None,
                                            },
                                            "initial_supply": 10000000,
                                            "token_distribution_date": "2020-04-17T05:00:00Z",
                                        },
                                    },
                                    "token": {
                                        "multitoken": [],
                                        "token_usage": "Vote",
                                        "token_name": "Compound governance token",
                                        "token_address": "0xc00e94cb662c3520282e6f5717214004a7f26888",
                                        "token_type": "ERC-20",
                                        "token_usage_details": "The COMP token is used ...",
                                        "block_explorers": None,
                                    },
                                },
                                "metadata": {"updated_at": "2021-07-24T08:23:17Z"},
                                "ecosystem": {
                                    "assets": [
                                        {
                                            "name": "Maker",
                                            "id": "6d06be66-d8c6-4e24-adfb-779b1afc3c9a",
                                        }
                                    ],
                                    "organizations": [],
                                },
                                "advisors": {
                                    "organizations": [],
                                    "individuals": [],
                                },
                                "contributors": {
                                    "organizations": [
                                        {
                                            "name": "Compound",
                                            "logo": "https://messari.s3.amazonaws.com/images/agora-images/0%3Fe%3D1559174400%26v%3Dbeta%26t%3DIzRX1KSfPGlT_Q0nXZTpYgFhg9GkYJEMpUAqKLCQN9M",
                                            "description": "Compound is a decentralized protocol ...",
                                            "slug": "compound",
                                        }
                                    ],
                                    "individuals": [
                                        {
                                            "avatar_url": "https://messari.s3.amazonaws.com/images/agora-images/0%3Fe%3D1554336000%26v%3Dbeta%26t%3DYwyHmqgg_RHYCrnb9nioPtfAnVHJB3p9CgXb-vqY5tc",
                                            "last_name": "Hayes",
                                            "description": None,
                                            "title": "CTO of Compound",
                                            "first_name": "Geoffrey",
                                            "slug": "geoffrey-hayes",
                                        },
                                    ],
                                },
                                "technology": {
                                    "overview": {
                                        "client_repositories": [],
                                        "technology_details": "Compound pools are created by users supplying assets in...",
                                    },
                                    "security": {
                                        "known_exploits_and_vulnerabilities": [
                                            {
                                                "date": "2018-12-09T05:00:00Z",
                                                "details": "An outside party discovered a defect...",
                                                "title": "Potential exploit",
                                                "type": "Bug disclosure",
                                            }
                                        ],
                                        "audits": [
                                            {
                                                "date": "2019-04-08T04:00:00Z",
                                                "details": "Trail of Bits [audited](https://github.com/trailofbits/publications/blob/master/reviews/compound-2.pdf) Compound's smart contracts ... ",
                                                "title": "Compound v2 Security Assessment",
                                                "type": None,
                                            }
                                        ],
                                    },
                                },
                                "investors": {
                                    "organizations": [
                                        {
                                            "name": "Coinbase Ventures",
                                            "logo": "https://messari.s3.amazonaws.com/images/agora-images/HVEeWmV4_400x400.png",
                                            "description": "Coinbase Ventures invests in ...",
                                            "slug": "coinbase-ventures",
                                        }
                                    ],
                                    "individuals": [],
                                },
                                "governance": {
                                    "grants": [],
                                    "onchain_governance": {
                                        "is_treasury_decentralized": None,
                                        "onchain_governance_details": "Anyone with 1% of tokens held or delegated to ...",
                                        "onchain_governance_type": "Direct On-Chain Vote, Delegated On-Chain Vote",
                                    },
                                    "governance_details": "Compound is governed entirely on-chain by COMP holders where ...",
                                },
                            },
                            "serial_id": 350,
                            "name": "Compound",
                            "id": "157f4fe3-6046-4b6d-bceb-a2af8ca021b5",
                            "contract_addresses": [
                                {
                                    "contract_address": "0x52ce071bd9b1c4b00a0b92d298c512478cad67e8",
                                    "platform": "binance-smart-chain",
                                }
                            ],
                            "slug": "compound",
                        },
                        "status": {
                            "elapsed": 2,
                            "timestamp": "2023-08-03T18:34:22.085942893Z",
                        },
                    }
                }
            },
        },
        204: {
            "description": "No content found.",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def asset_profile(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the asset profile data from Messari.

    """

    try:
        ref = db.collection(f"{protocol_name}-messari").document(f"asset_profile").get(field_paths=["data"]).to_dict()

        if ref is None:
            raise exceptions.NotFound("Collection or document not found")

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@router.get(
    "/{protocol_name}/metrics",
    tags=["Messari - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Messari metrics data",
            "content": {
                "application/json": {
                    "example": {
                        "data": {
                            "marketcap": {
                                "outstanding_marketcap_usd": 620156823.022705,
                                "liquid_marketcap_usd": 519758935.14514136,
                                "current_marketcap_usd": 409839830.6858402,
                                "marketcap_dominance_percent": 0.03409938740189166,
                                "realized_marketcap_usd": 830935523.8075892,
                                "rank": 100,
                                "y_2050_marketcap_usd": 597775239.1607776,
                                "volume_turnover_last_24_hours_percent": 13.20424173543578,
                                "y_plus10_marketcap_usd": 597775239.1607776,
                            },
                            "symbol": "COMP",
                            "supply_activity": {
                                "outstanding": None,
                                "supply_revived_2y": None,
                                "supply_revived_30d": None,
                                "supply_active_10y": 10000000,
                                "supply_revived_3y": None,
                                "supply_revived_4y": None,
                                "supply_revived_5y": None,
                                "supply_active_1y_percent": 78.713158897,
                                "supply_active_5y": 10000000,
                                "supply_active_30d": 5599983.319682396,
                                "supply_active_4y": 10000000,
                                "supply_active_3y": 9354878.551488778,
                                "supply_revived_1y": None,
                                "supply_revived_7d": None,
                                "supply_revived_90d": None,
                                "supply_active_90d": 6684090.390193483,
                                "supply_active_7d": 4029237.403186421,
                                "supply_active_ever": 10000000,
                                "supply_active_2y": 8741839.233954992,
                                "supply_active_1y": 7871315.889700713,
                                "supply_active_180d": 7136970.088110352,
                                "supply_active_1d": 3739927.788576772,
                            },
                            "all_time_high": {
                                "percent_down": -93.33990723306779,
                                "at": "2021-05-12T01:00:00Z",
                                "price": 905.1797142265216,
                                "days_since": 813,
                                "breakeven_multiple": 15.014805874252428,
                            },
                            "roi_data": {
                                "percent_change_last_1_week": -14.111830626725808,
                                "percent_change_btc_last_1_month": 1.4664804063154413,
                                "percent_change_last_1_month": -3.6079481532496995,
                                "percent_change_year_to_date": 91.49725474120488,
                                "percent_change_btc_last_1_week": -14.15603776451461,
                                "percent_change_last_1_year": 6.567877277512545,
                                "percent_change_btc_last_1_year": -16.762136970822635,
                                "percent_change_eth_last_1_week": -13.520045616560353,
                                "percent_change_month_to_date": -8.434394170372668,
                                "percent_change_btc_last_3_months": 49.683707801640935,
                                "percent_change_eth_last_3_months": 59.93476556318819,
                                "percent_change_eth_last_1_year": -6.65378736574656,
                                "percent_change_last_3_months": 48.11923792167192,
                                "percent_change_eth_last_1_month": 0.9753543275021778,
                                "percent_change_quarter_to_date": 14.963574978971039,
                            },
                            "supply": {
                                "circulating": 6856085.76329154,
                                "y_2050_issued_percent": 86.94889,
                                "y_2050": 10000000,
                                "liquid": 8694889,
                                "stock_to_flow": 6.662183523087308,
                                "y_plus10_issued_percent": 86.94889,
                                "y_plus10": 10000000,
                                "annual_inflation_percent": 15.010093860887702,
                                "supply_revived_90d": 10000000,
                            },
                            "cycle_low": {
                                "at": "2023-06-10T16:00:00Z",
                                "price": 25.827885552798527,
                                "percent_up": 133.41364337277759,
                                "days_since": 54,
                            },
                            "alert_messages": None,
                            "roi_by_year": {
                                "2021_usd_percent": 37.157090154113234,
                                "2012_usd_percent": 0,
                                "2018_usd_percent": 0,
                                "2011_usd_percent": 0,
                                "2015_usd_percent": 0,
                                "2019_usd_percent": 0,
                                "2014_usd_percent": 0,
                                "2016_usd_percent": 0,
                                "2020_usd_percent": None,
                                "2013_usd_percent": 0,
                                "2017_usd_percent": 0,
                            },
                            "token_sale_stats": {
                                "sale_proceeds_usd": None,
                                "sale_end_date": None,
                                "roi_since_sale_btc_percent": None,
                                "roi_since_sale_usd_percent": None,
                                "roi_since_sale_eth_percent": None,
                                "sale_start_date": None,
                            },
                            "serial_id": 350,
                            "reddit": {
                                "active_user_count": None,
                                "subscribers": None,
                            },
                            "misc_data": {
                                "asset_created_at": None,
                                "vladimir_club_cost": None,
                                "sectors": ["Lending"],
                                "btc_y2050_normalized_supply_price_usd": None,
                                "private_market_price_usd": None,
                                "asset_age_days": None,
                                "btc_current_normalized_supply_price_usd": None,
                                "categories": ["Financial"],
                                "tags": ["DeFi"],
                            },
                            "risk_metrics": {
                                "volatility_stats": {
                                    "volatility_last_1_year": 1.0479291787678886,
                                    "volatility_last_90_days": 1.3353319113065794,
                                    "volatility_last_30_days": 1.145703683617018,
                                    "volatility_last_3_years": 1.2420971735487798,
                                },
                                "sharpe_ratios": {
                                    "last_1_year": 0.5220986301173018,
                                    "last_90_days": 1.8101719209507514,
                                    "last_30_days": -0.05646285403953599,
                                    "last_3_years": 0.38147803461305513,
                                },
                            },
                            "id": "157f4fe3-6046-4b6d-bceb-a2af8ca021b5",
                            "on_chain_data": {
                                "txn_erc20_count_last_24_hours": None,
                                "hash_rate": None,
                                "txn_erc721_count_last_24_hours": None,
                                "realized_marketcap_usd": 830935523.8075892,
                                "adjusted_rvt": 21.851188377407,
                                "average_transfer_value_usd": 42576.74885282842,
                                "addresses_balance_greater_10_usd_count": 57914,
                                "block_height": None,
                                "adjusted_nvt_90d_moving_average": 50.60007862806452,
                                "adjusted_nvt": 16.308321374092,
                                "average_fee_native_units": None,
                                "average_transfer_value_native_units": 686.548099967766,
                                "transfer_count_last_24_hours": 1031,
                                "txn_contracts_count_last_24_hours": 394,
                                "addresses_balance_greater_1m_usd_count": 94,
                                "value_weighted_average_utxo_age": None,
                                "addresses_balance_greater_1_native_units_count": 31930,
                                "utxo_in_profit_count": None,
                                "average_txn_gas_used": None,
                                "issuance_rate": None,
                                "addresses_balance_greater_0_01_native_units_count": 158145,
                                "average_block_interval": None,
                                "addresses_balance_greater_10k_usd_count": 741,
                                "addresses_balance_greater_10m_usd_count": 11,
                                "txn_per_second_count": 0.010150462963,
                                "addresses_balance_greater_10k_native_units_count": 112,
                                "addresses_balance_greater_100_usd_count": 24567,
                                "new_issuance": None,
                                "txn_token_count_last_24_hours": None,
                                "average_block_weight": None,
                                "txn_gas_limit_last_24_hours": None,
                                "addresses_balance_greater_10_native_units_count": 6367,
                                "average_block_gas_limit": None,
                                "transfer_erc721_count_last_24_hours": None,
                                "txn_contracts_calls_count_last_24_hours": None,
                                "txn_count_last_24_hours": 877,
                                "issuance_total_usd": 0,
                                "uncle_rewards_last_24_hours_usd": None,
                                "txn_contracts_destruction_count_last_24_hours": None,
                                "uncle_rewards_last_24_hours_native_units": None,
                                "total_fees_last_24_hours": None,
                                "adjusted_txn_volume_last_24_hours_native_units": 613183.8937075749,
                                "active_addresses_received_count": 382,
                                "addresses_balance_greater_1_usd_count": 140555,
                                "median_fee_native_units": None,
                                "addresses_count": 213325,
                                "block_weight": None,
                                "txn_gas_used_last_24_hours": None,
                                "txn_contracts_calls_success_count_last_24_hours": None,
                                "utxo_in_loss_count": None,
                                "average_txn_gas_limit": None,
                                "average_utxo_age": None,
                                "median_transfer_value_native_units": 71.38721459145525,
                                "addresses_balance_greater_1k_usd_count": 4336,
                                "addresses_balance_greater_0_1_native_units_count": 68914,
                                "adjusted_rvt_90d_moving_average": 67.79801701541005,
                                "txn_contracts_creation_count_last_24_hours": None,
                                "active_addresses_sent_count": 319,
                                "issuance_rate_daily": None,
                                "addresses_balance_greater_0_001_native_units_count": 188068,
                                "addresses_balance_greater_100k_usd_count": 228,
                                "uncle_blocks_count_last_24_hours": None,
                                "median_transfer_value_usd": 4427.126820547698,
                                "average_fee_usd": None,
                                "issuance_total_native_units": 0,
                                "issuance_last_24_hours_native_units": None,
                                "transfer_erc_20_count_last_24_hours": None,
                                "adjusted_txn_volume_last_24_hours_usd": 38027017.54503817,
                                "active_addresses": 479,
                                "utxo_count_last_24_hours": None,
                                "median_utxo_age": None,
                                "blocks_added_last_24_hours": None,
                                "median_fee_usd": None,
                                "addresses_balance_greater_100_native_units_count": 1066,
                                "mining_revenue_from_uncle_blocks_percent_last_24_hours": None,
                                "txn_volume_last_24_hours_native_units": 707831.0910667668,
                                "addresses_balance_greater_1m_native_units_count": 1,
                                "block_size_bytes_total": None,
                                "addresses_balance_greater_100k_native_units_count": 14,
                                "addresses_balance_greater_1k_native_units_count": 279,
                                "block_size_bytes_average": None,
                                "total_fees_last_24_hours_usd": None,
                                "txn_volume_last_24_hours_usd": 43896628.06726611,
                            },
                            "slug": "compound",
                            "developer_activity": {
                                "lines_added_last_1_year": None,
                                "commits_last_1_year": None,
                                "lines_added_last_3_months": None,
                                "lines_deleted_last_1_year": None,
                                "watchers": None,
                                "lines_deleted_last_3_months": None,
                                "stars": None,
                                "commits_last_3_months": None,
                            },
                            "_internal_temp_agora_id": "b4a4024c-14c1-4e90-b092-b27cb58b7521",
                            "mining_stats": {
                                "hash_rate": None,
                                "attack_appeal": None,
                                "mining_revenue_from_fees_percent_last_24_hours": None,
                                "network_hash_rate": None,
                                "mining_revenue_total": None,
                                "hash_rate_30d_average": None,
                                "available_on_nicehash_percent": None,
                                "average_difficulty": None,
                                "24_hours_attack_cost": None,
                                "mining_revenue_per_hash_native_units": None,
                                "mining_revenue_per_hash_per_second_native_units": None,
                                "mining_algo": None,
                                "mining_revenue_per_hash_per_second_usd": None,
                                "mining_revenue_per_hash_usd": None,
                                "mining_revenue_usd": None,
                                "1_hour_attack_cost": None,
                                "mining_revenue_native": None,
                            },
                            "market_data": {
                                "price_usd": 59.777523916077755,
                                "percent_change_btc_last_1_hour": 0.038696074800665836,
                                "percent_change_eth_last_24_hours": -5.298735742484862,
                                "percent_change_usd_last_1_hour": -0.4000698837566756,
                                "percent_change_btc_last_24_hours": -4.961118642141951,
                                "price_btc": 0.0020503463110269037,
                                "percent_change_usd_last_24_hours": -4.883462903234547,
                                "volume_last_24_hours_overstatement_multiple": 0.9987183556965813,
                                "last_trade_at": "2023-08-03T18:22:00Z",
                                "price_eth": 0.03247828838607769,
                                "real_volume_last_24_hours": 68630226.23809133,
                                "ohlcv_last_24_hour": {
                                    "volume": 68630226.23809133,
                                    "high": 63.893809223186324,
                                    "low": 58.69218657254978,
                                    "close": 59.777523916077755,
                                    "open": 63.0053017836613,
                                },
                                "percent_change_eth_last_1_hour": 0.21899233995534217,
                                "ohlcv_last_1_hour": {
                                    "volume": 807210.6209049412,
                                    "high": 60.03510581968828,
                                    "low": 59.71934145210464,
                                    "close": 59.777523916077755,
                                    "open": 59.94327212848452,
                                },
                                "volume_last_24_hours": 68542266.69959095,
                            },
                            "supply_distribution": {
                                "supply_in_addresses_balance_greater_10k_native_units": 8989878.05105035,
                                "supply_in_top_10_percent_addresses": 9968213.353461752,
                                "supply_in_addresses_balance_greater_0_01_native_units": 9999883.87804697,
                                "supply_in_addresses_balance_greater_1_native_units": 9983168.586671175,
                                "supply_in_addresses_balance_greater_100_native_units": 9756674.46800815,
                                "supply_in_top_1_percent_addresses": 9822040.606222052,
                                "supply_in_addresses_balance_greater_10k_usd": 9716556.529407993,
                                "supply_in_addresses_balance_greater_1m_usd": 8772281.454368787,
                                "supply_in_addresses_balance_greater_100_usd": 9974206.797289645,
                                "supply_in_addresses_balance_greater_100k_usd": 9484507.892288864,
                                "supply_in_utxo_in_loss": None,
                                "supply_in_addresses_balance_greater_10m_usd": 4975330.498556008,
                                "supply_in_addresses_balance_greater_1_usd": 9999660.801402632,
                                "supply_in_addresses_balance_greater_0_1_native_units": 9996350.798762387,
                                "supply_in_contracts_native_units": 3554718.3765288335,
                                "supply_in_addresses_balance_greater_0_001_native_units": 9999991.651076272,
                                "supply_in_contracts_usd": 220448285.5128549,
                                "supply_in_addresses_balance_greater_10_native_units": 9901732.770603864,
                                "supply_in_utxo_in_profit": None,
                                "supply_in_top_100_addresses": 8858464.983067088,
                                "supply_shielded": None,
                                "supply_in_addresses_balance_greater_1k_native_units": 9550087.80517032,
                                "supply_in_addresses_balance_greater_100k_native_units": 5314450.344210118,
                                "supply_in_addresses_balance_greater_1k_usd": 9876386.902556144,
                                "supply_in_addresses_balance_greater_1m_native_units": 1407057.5420756098,
                                "supply_in_addresses_balance_greater_10_usd": 9994941.288516559,
                            },
                            "exchange_flows": {
                                "flow_out_gemini_usd": None,
                                "flow_out_bitfinex_native_units": None,
                                "supply_huobi_native_units": None,
                                "flow_in_bitstamp_native_units": None,
                                "flow_net_bitmex_usd": None,
                                "flow_out_exchange_usd_inclusive": None,
                                "supply_bitfinex_usd": None,
                                "flow_net_bittrex_native_units": None,
                                "flow_out_poloniex_usd": None,
                                "flow_in_exchange_native_units": None,
                                "flow_net_bitmex_native_units": None,
                                "flow_in_poloniex_native_units": None,
                                "flow_out_poloniex_native_units": None,
                                "flow_net_binance_native_units": None,
                                "flow_in_kraken_usd": None,
                                "flow_in_exchange_native_units_inclusive": None,
                                "flow_net_poloniex_native_units": None,
                                "flow_out_gemini_native_units": None,
                                "flow_net_kraken_native_units": None,
                                "flow_out_exchange_native_units_inclusive": None,
                                "flow_in_gemini_native_units": None,
                                "supply_bitstamp_usd": None,
                                "flow_net_poloniex_usd": None,
                                "supply_exchange_usd": None,
                                "flow_in_bitmex_usd": None,
                                "flow_out_bitmex_usd": None,
                                "flow_in_bitfinex_native_units": None,
                                "flow_in_kraken_native_units": None,
                                "supply_binance_usd": None,
                                "flow_net_bitfinex_native_units": None,
                                "flow_net_binance_usd": None,
                                "supply_poloniex_native_units": None,
                                "supply_poloniex_usd": None,
                                "supply_bitmex_usd": None,
                                "flow_in_binance_usd": None,
                                "flow_net_bitfinex_usd": None,
                                "supply_kraken_usd": None,
                                "supply_bitfinex_native_units": None,
                                "flow_net_bitstamp_native_units": None,
                                "flow_net_gemini_usd": None,
                                "flow_out_binance_native_units": None,
                                "flow_in_bittrex_usd": None,
                                "flow_in_huobi_native_units": None,
                                "flow_in_bitfinex_usd": None,
                                "flow_out_kraken_usd": None,
                                "flow_out_bitfinex_usd": None,
                                "flow_in_huobi_usd": None,
                                "flow_in_binance_native_units": None,
                                "flow_out_huobi_native_units": None,
                                "supply_kraken_native_units": None,
                                "flow_net_huobi_native_units": None,
                                "flow_net_huobi_usd": None,
                                "flow_in_gemini_usd": None,
                                "flow_net_bitstamp_usd": None,
                                "supply_binance_native_units": None,
                                "supply_bittrex_native_units": None,
                                "supply_bitstamp_native_units": None,
                                "flow_out_bitstamp_usd": None,
                                "flow_out_kraken_native_units": None,
                                "flow_net_gemini_native_units": None,
                                "supply_bitmex_native_units": None,
                                "flow_in_bitmex_native_units": None,
                                "flow_out_bittrex_usd": None,
                                "flow_out_exchange_native_units": None,
                                "flow_out_bittrex_native_units": None,
                                "flow_out_bitstamp_native_units": None,
                                "flow_in_exchange_usd_inclusive": None,
                                "flow_out_binance_usd": None,
                                "flow_in_bitstamp_usd": None,
                                "flow_net_kraken_usd": None,
                                "supply_gemini_usd": None,
                                "flow_out_bitmex_native_units": None,
                                "supply_huobi_usd": None,
                                "flow_in_exchange_usd": None,
                                "supply_bittrex_usd": None,
                                "supply_gemini_native_units": None,
                                "flow_net_bittrex_usd": None,
                                "flow_out_exchange_usd": None,
                                "flow_in_bittrex_native_units": None,
                                "flow_in_poloniex_usd": None,
                                "supply_exchange_native_units": None,
                                "flow_out_huobi_usd": None,
                            },
                            "miner_flows": {
                                "supply_1hop_miners_native_units": None,
                                "supply_miners_native_units": None,
                                "supply_1hop_miners_usd": None,
                                "supply_miners_usd": None,
                            },
                            "name": "Compound",
                            "blockchain_stats_24_hours": {
                                "adjusted_transaction_volume": 38027017.54503817,
                                "count_of_tx": 877,
                                "transaction_volume": 43896628.06726611,
                                "new_issuance": None,
                                "average_difficulty": None,
                                "median_tx_value": 4427.126820547698,
                                "count_of_blocks_added": None,
                                "adjusted_nvt": 16.308321374092,
                                "count_of_payments": 1031,
                                "kilobytes_added": None,
                                "median_tx_fee": None,
                                "count_of_active_addresses": 479,
                            },
                            "contract_addresses": [
                                {
                                    "contract_address": "c00e94cb662c3520282e6f5717214004a7f26888.factory.bridge.near",
                                    "platform": "near-protocol",
                                }
                            ],
                            "market_data_liquidity": {
                                "asset_bid_depth": None,
                                "marketcap": None,
                                "updated_at": "0001-01-01T00:00:00Z",
                                "usd_bid_depth": None,
                                "clearing_prices_to_sell": None,
                            },
                        },
                        "status": {
                            "elapsed": 1,
                            "timestamp": "2023-08-03T18:34:22.800607564Z",
                        },
                    }
                }
            },
        },
        204: {
            "description": "No content found.",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def asset_metrics(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the asset metrics data from Messari.

    """

    try:
        ref = db.collection(f"{protocol_name}-messari").document(f"asset_metrics").get(field_paths=["data"]).to_dict()

        if ref is None:
            raise exceptions.NotFound("Collection or document not found")

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@router.get(
    "/{protocol_name}/indexed-timeseries-list",
    tags=["Messari - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Indexed timeseries list",
            "content": {
                "application/json": {
                    "example": [
                        "asset_timeseries_act.addr.cnt_1d",
                        "asset_timeseries_act.addr.cnt_1w",
                        "asset_timeseries_daily.shp_1d",
                        "asset_timeseries_daily.shp_1w",
                        "asset_timeseries_daily.vol_1d",
                    ]
                }
            },
        },
        204: {
            "description": "No content found.",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def indexed_timeseries_list(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the list of indexed timeseries names.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-messari")
            .document(f"indexed_timeseries_list")
            .get(field_paths=["data"])
            .to_dict()
        )

        if ref is None:
            raise exceptions.NotFound("Collection or document not found")

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data


@router.get(
    "/{protocol_name}/timeseries",
    tags=["Messari - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Timeseries data",
            "content": {
                "application/json": {
                    "example": "Returns the timeseries data that is given as a parameter. Timeseries can be found in here: `https://data.messari.io/api/v1/assets/metrics`."
                }
            },
        },
        204: {
            "description": "No content found.",
            "content": {"application/json": {"example": None}},
        },
        404: {
            "description": "Not found",
            "content": {"application/json": {"example": {"error": "Error description"}}},
        },
    },
)
def timeseries(
    protocol_name: str = Path(..., description="Protocol name"),
    timeseries_name: str = Query(..., description="Timeseries name"),
):
    """
    Returns the timeseries data that is given as a parameter.

    """

    try:
        ref = db.collection(f"{protocol_name}-messari").document(timeseries_name).get(field_paths=["data"]).to_dict()

        if ref is None:
            raise exceptions.NotFound("Collection or document not found")

    except exceptions.NotFound as ex:
        # Handle case where document or collection does not exist
        raise HTTPException(status_code=404, detail=str(ex))

    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred {str(e)}")

    data = ref.get("data", None)
    if not data:
        raise HTTPException(status_code=204, detail="Content is empty.")

    return data
