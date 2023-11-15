from fastapi import APIRouter
from fastapi import Depends, HTTPException
from enum import Enum
from fastapi import HTTPException
from fastapi import Query, Path, HTTPException
from google.cloud import exceptions
from fastapi import Depends
from ..api import get_current_user, db

router = APIRouter()


class VotingPowerInterval(Enum):
    WEEK = "WEEK"
    MONTH = "MONTH"
    YEAR = "YEAR"


@router.get(
    "/{protocol_name}/voting-power-chart",
    tags=["Governance - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Voting Power Chart",
            "content": {
                "application/json": {
                    "example": {
                        "yAxis": {"type": "value"},
                        "xAxis": {"type": "time"},
                        "series": [
                            {
                                "twitter": "",
                                "address": "0xea6C3Db2e7FCA00Ea9d7211a03e83f568Fc13BF7",
                                "data": [
                                    {
                                        "balance": 331.07,
                                        "timestamp": "2023-01-01T00:00:00Z",
                                    }
                                ],
                                "name": "Polychain Capital",
                                "bio": "",
                                "tally_url": "https://www.tally.xyz/profile/0xea6C3Db2e7FCA00Ea9d7211a03e83f568Fc13BF7?governanceId=eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                                "ens": "",
                                "type": "line",
                                "picture": "https://static.tally.xyz/7b888910-fdfb-40af-84b1-09847c6054b2_400x400.jpg",
                                "email": "",
                            },
                        ],
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def voting_power_chart(
    protocol_name: str = Path(..., description="Protocol name"),
    interval: VotingPowerInterval = Query(
        VotingPowerInterval.WEEK, description="Interval to group by"
    ),
):
    """
    Returns the voting power chart.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-governance")
            .document(f"voting_power_chart_{interval.value.lower()}")
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


class DelegateSortField(Enum):
    CREATED = "CREATED"
    UPDATED = "UPDATED"
    TOKENS_OWNED = "TOKENS_OWNED"
    VOTING_WEIGHT = "VOTING_WEIGHT"
    DELEGATIONS = "DELEGATIONS"
    HAS_ENS = "HAS_ENS"
    HAS_DELEGATE_STATEMENT = "HAS_DELEGATE_STATEMENT"
    PROPOSALS_CREATED = "PROPOSALS_CREATED"
    VOTES_CAST = "VOTES_CAST"


@router.get(
    "/{protocol_name}/delegates",
    tags=["Governance - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Delegates",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "participation": {
                                "stats": {
                                    "delegationCount": 162,
                                    "votingPower": {
                                        "in": 330.94,
                                        "net": 330.94,
                                        "out": 0,
                                    },
                                    "activeDelegationCount": 119,
                                    "weight": {"total": 330.94, "owned": 0},
                                    "votes": {"total": 47},
                                    "voteCount": 47,
                                    "delegations": {"total": 162},
                                    "recentParticipationRate": {
                                        "recentProposalCount": 10,
                                        "recentVoteCount": 8,
                                    },
                                    "tokenBalance": 0,
                                    "createdProposalsCount": 0,
                                }
                            },
                            "account": {
                                "twitter": "",
                                "address": "0xea6C3Db2e7FCA00Ea9d7211a03e83f568Fc13BF7",
                                "name": "Polychain Capital",
                                "bio": "",
                                "tally_url": "https://www.tally.xyz/profile/0xea6C3Db2e7FCA00Ea9d7211a03e83f568Fc13BF7?governanceId=eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                                "id": "eip155:1:0xea6C3Db2e7FCA00Ea9d7211a03e83f568Fc13BF7",
                                "ens": "",
                                "picture": "https://static.tally.xyz/7b888910-fdfb-40af-84b1-09847c6054b2_400x400.jpg",
                                "email": "",
                            },
                        }
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def delegates(
    protocol_name: str = Path(..., description="Protocol name"),
    sort_by: DelegateSortField = Query(
        DelegateSortField.VOTING_WEIGHT, description="Sort by"
    ),
):
    """
    Returns the delegates sorted by the given field.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-governance")
            .document(f"delegates_{sort_by.value.lower()}")
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
    "/{protocol_name}/proposals",
    tags=["Governance - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Governance Proposals",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "proposer": {
                                "twitter": "",
                                "address": "0x7B3c54e17d618CC94daDFe7671c1e2F50C4Ecc33",
                                "name": "0x7B3c54e17d618CC94daDFe7671c1e2F50C4Ecc33",
                                "bio": "",
                                "tally_url": "https://www.tally.xyz/profile/0x7B3c54e17d618CC94daDFe7671c1e2F50C4Ecc33?governanceId=eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                                "id": "eip155:1:0x7B3c54e17d618CC94daDFe7671c1e2F50C4Ecc33",
                                "ens": "",
                                "picture": None,
                                "email": "",
                            },
                            "start": {"timestamp": "2023-07-26T18:56:47Z"},
                            "createdTransaction": {
                                "block": {
                                    "number": 17766046,
                                    "timestamp": "2023-07-24T22:48:59Z",
                                }
                            },
                            "description": "# [Gauntlet] 2023-07-24 Pause supply for Compound v2 Tail Assets...",
                            "statusChanges": [
                                {
                                    "type": "PENDING",
                                    "txHash": "0x410ebdda821c1b7011f519b22cb3a681443be6ba02da0ce2e5bc23c377996e84",
                                    "transaction": {
                                        "block": {
                                            "number": 17766046,
                                            "timestamp": "2023-07-24T22:48:59Z",
                                        }
                                    },
                                },
                                {
                                    "type": "ACTIVE",
                                    "txHash": "",
                                    "transaction": None,
                                },
                                {
                                    "type": "SUCCEEDED",
                                    "txHash": "",
                                    "transaction": None,
                                },
                                {
                                    "type": "QUEUED",
                                    "txHash": "0x5b3de1ae78d5eb18f7001a361f6b6ede8cde50b27ea5a1dabd48b42e3519a6b3",
                                    "transaction": {
                                        "block": {
                                            "number": 17798899,
                                            "timestamp": "2023-07-29T13:09:35Z",
                                        }
                                    },
                                },
                                {
                                    "type": "EXECUTED",
                                    "txHash": "0xf6ae0641f32b2ad24ec576fe71a1cd0ffb122f00b8cd46ae4741830ceae37bfe",
                                    "transaction": {
                                        "block": {
                                            "number": 17813201,
                                            "timestamp": "2023-07-31T13:10:35Z",
                                        }
                                    },
                                },
                            ],
                            "tally_url": "https://www.tally.xyz/gov/compound/proposal/170",
                            "title": "# [Gauntlet] 2023-07-24 Pause supply for Compound v2 Tail Assets",
                            "executable": {
                                "values": ["0", "0", "0", "0", "0"],
                                "callDatas": [
                                    "0x000000000000000000000000e65cdb6479bac1e22340e4e755fae7e509ecd06c0000000000000000000000000000000000000000000000000000000000000001",
                                    "0x00000000000000000000000070e36f6bf80a52b3b46b3af8e106cc0ed743e8e40000000000000000000000000000000000000000000000000000000000000001",
                                    "0x000000000000000000000000face851a4921ce59e912d19329929ce6da6eb0c70000000000000000000000000000000000000000000000000000000000000001",
                                    "0x0000000000000000000000004b0181102a0112a2ef11abee5563bb4a3176c9d70000000000000000000000000000000000000000000000000000000000000001",
                                    "0x00000000000000000000000035a18000230da775cac24873d00ff85bccded5500000000000000000000000000000000000000000000000000000000000000001",
                                ],
                                "targets": [
                                    "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",
                                    "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",
                                    "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",
                                    "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",
                                    "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",
                                ],
                                "signatures": [
                                    "_setMintPaused(address,bool)",
                                    "_setMintPaused(address,bool)",
                                    "_setMintPaused(address,bool)",
                                    "_setMintPaused(address,bool)",
                                    "_setMintPaused(address,bool)",
                                ],
                            },
                            "eta": "1690808975",
                            "voteStats": [
                                {
                                    "weight": 810.03,
                                    "votes": "29",
                                    "support": "FOR",
                                    "percent": 100,
                                },
                                {
                                    "weight": 0,
                                    "votes": "0",
                                    "support": "AGAINST",
                                    "percent": 0,
                                },
                                {
                                    "weight": 0,
                                    "votes": "0",
                                    "support": "ABSTAIN",
                                    "percent": 0,
                                },
                            ],
                            "end": {"timestamp": "2023-07-29T13:08:59Z"},
                            "block": {
                                "number": 17766046,
                                "timestamp": "2023-07-24T22:48:59Z",
                            },
                            "votes": [
                                {
                                    "reason": "",
                                    "weight": 50,
                                    "tally_url": "https://www.tally.xyz/profile/0xed11e5eA95a5A3440fbAadc4CC404C56D0a5bb04?governanceId=eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                                    "voter": {
                                        "twitter": "she256.eth",
                                        "address": "0xed11e5eA95a5A3440fbAadc4CC404C56D0a5bb04",
                                        "name": "she256.eth",
                                        "bio": "",
                                        "id": "eip155:1:0xed11e5eA95a5A3440fbAadc4CC404C56D0a5bb04",
                                        "ens": "she256.eth",
                                        "picture": None,
                                        "email": "",
                                    },
                                    "support": "FOR",
                                    "transaction": {
                                        "block": {
                                            "number": 17792597,
                                            "timestamp": "2023-07-28T15:59:11Z",
                                        }
                                    },
                                }
                            ],
                            "governor": {"quorum": "400000000000000000000000"},
                            "id": 170,
                        }
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def proposals(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the governance proposals.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-governance")
            .document("proposals")
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
    "/{protocol_name}/info",
    tags=["Governance - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Governance Info",
            "content": {
                "application/json": {
                    "example": {
                        "chainId": "eip155:1",
                        "stats": {
                            "tokens": {
                                "delegatedVotingPower": 2112.06,
                                "voters": 4911,
                                "owners": 213260,
                                "delegates": {"total": 4911},
                                "supply": 10000,
                            },
                            "proposals": {
                                "total": 128,
                                "active": 0,
                                "failed": 32,
                                "passed": 96,
                            },
                        },
                        "kind": "SINGLE_GOV",
                        "organization": {
                            "website": "https://compound.finance/",
                            "name": "Compound",
                            "description": "Compound is an algorithmic, autonomous interest rate protocol built for developers, to unlock a universe of open financial applications.",
                            "visual": {
                                "color": "#00d395",
                                "icon": "https://static.withtally.com/bc952927-da93-4cab-b0ce-b5e2f5976b9a_400x400.jpg",
                            },
                            "tally_url": "https://www.tally.xyz/gov/compound",
                            "id": "1",
                            "slug": "compound",
                            "votingParameters": {
                                "bigVotingPeriod": "19710",
                                "quorum": 400,
                                "votingPeriod": 0,
                            },
                        },
                        "timelockId": "eip155:1:0x6d903f6003cca6255D85CcA4D3B5E5146dC33925",
                        "active": True,
                        "tokens": [
                            {
                                "symbol": "COMP",
                                "lastIndexedBlock": {
                                    "number": 17835154,
                                    "timestamp": "2023-08-03T14:46:47Z",
                                },
                                "isIndexing": True,
                                "decimals": 18,
                                "name": "Compound",
                                "id": "eip155:1/erc20:0xc00e94Cb662C3520282E6f5717214004A7f26888",
                                "type": "ERC20",
                            }
                        ],
                        "id": "eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                        "contracts": {
                            "timelock": {
                                "address": "0x6d903f6003cca6255D85CcA4D3B5E5146dC33925"
                            },
                            "tokens": [
                                {
                                    "address": "0xc00e94Cb662C3520282E6f5717214004A7f26888"
                                }
                            ],
                            "governor": {
                                "address": "0xc0Da02939E1441F497fd74F78cE7Decb17B66529"
                            },
                        },
                        "proposalThreshold": 25,
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def info(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the governance info.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-governance")
            .document("governance_info")
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
    "/{protocol_name}/safes",
    tags=["Governance - Ecosystem"],
    dependencies=[Depends(get_current_user)],
    responses={
        200: {
            "description": "Governance Safes",
            "content": {
                "application/json": {
                    "example": {
                        "gnosisSafes": [
                            {
                                "balance": {
                                    "totalUSDValue": "264059.3134",
                                    "tokens": [
                                        {
                                            "symbol": "COMP",
                                            "amount": "4370761897000000000000",
                                            "address": "0xc00e94Cb662C3520282E6f5717214004A7f26888",
                                            "decimals": 18,
                                            "name": "Compound",
                                            "fiat": "259415.0321",
                                            "logoURI": "https://safe-transaction-assets.safe.global/tokens/logos/0xc00e94Cb662C3520282E6f5717214004A7f26888.png",
                                        },
                                    ],
                                },
                                "name": "Compound Grants Program",
                                "threshold": 3,
                                "owners": [
                                    {
                                        "address": "0x66cD62c6F8A4BB0Cd8720488BCBd1A6221B765F9",
                                        "name": "allthecolors",
                                        "bio": "Here to help, I hope. Euler delegate; Compound governance participant; ad hoc contributor on dev and analytics for Compound and Aztec Connect; ardent non-maximalist",
                                        "tally_url": "https://www.tally.xyz/profile/0x66cD62c6F8A4BB0Cd8720488BCBd1A6221B765F9?governanceId=eip155:1:0xc0Da02939E1441F497fd74F78cE7Decb17B66529",
                                        "id": "eip155:1:0x66cD62c6F8A4BB0Cd8720488BCBd1A6221B765F9",
                                        "picture": "https://static.tally.xyz/10b676fb-6d18-45f8-ae15-6160b66bfc59_400x400.jpg",
                                    },
                                ],
                                "tally_url": "https://www.tally.xyz/safe/eip155:1:0x8524B12CB7710C75B53bAa9ca72B420542d24C13",
                                "id": "eip155:1:0x8524B12CB7710C75B53bAa9ca72B420542d24C13",
                                "nonce": 52,
                                "version": "1.3.0",
                            }
                        ],
                        "latestUpdate": "2023-08-03T14:40:04.760Z",
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
            "content": {
                "application/json": {"example": {"error": "Error description"}}
            },
        },
    },
)
def safes(
    protocol_name: str = Path(..., description="Protocol name"),
):
    """
    Returns the governance Gnosis safes.

    """

    try:
        ref = (
            db.collection(f"{protocol_name}-governance")
            .document("safes")
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
