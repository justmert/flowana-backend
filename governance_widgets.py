from discourse_actor import DiscourseActor
import logging
import log_config
import json
from datetime import datetime
from enum import Enum
import pandas as pd
from datetime import timedelta
from governance_actor import GovernanceActor
logger = logging.getLogger(__name__)


class GovernanceWidgets():

    def __init__(self, actor: GovernanceActor, collection_refs, governance_id, organization_id, chain_id, slug):
        self.actor = actor
        self.collection_refs = collection_refs
        self.governance_id = governance_id
        self.organization_id = organization_id
        self.chain_id = chain_id
        self.slug = slug

    def is_valid(self, response):
        if response is None:
            return False

        elif isinstance(response, dict) and not response:
            return False

        elif isinstance(response, list) and not response:
            return False

        return True


    class VotingPowerInterval(Enum):
        WEEK = "WEEK"
        MONTH = "MONTH"
        YEAR = "YEAR"
        
    def voting_power_chart(self, **kwargs):

        # all_intervals = {}
        for interval in self.VotingPowerInterval:
            query = """
                query GovernanceTopAdvocates(
                    $governanceId: AccountID!, 
                    $pagination: Pagination, 
                    $weightChangesSort: DelegationWeightChangeSort, 
                    $weightChangesInterval: TimeInterval, 
                    $weightChangesPagination: Pagination
                ) {
                    governance(id: $governanceId) {
                        delegates(pagination: $pagination) {
                            account {
                                id
                                name
                                picture
                                address
                                bio
                                email
                                twitter
                                ens
                            }
                            participation {
                                weightChanges(
                                    sort: $weightChangesSort
                                    interval: $weightChangesInterval
                                    pagination: $weightChangesPagination
                                ) {
                                    timestamp
                                    newBalance
                                    prevBalance
                                }
                            }
                        }
                    }
                }
            """
            variables = {
                "pagination": {
                    "limit": 10,
                    "offset": 0
                },
                "governanceId": self.governance_id,
                "weightChangesSort": {
                    "field": "CREATED",
                    "order": "DESC"
                },
                "weightChangesInterval": interval.value,
                "weightChangesPagination": {
                    "limit": 1000,
                    "offset": 0
                }
            }

            result = self.actor.governance_graphql_make_query(
                query, variables=variables)

            if not self.is_valid(result):
                logger.info(f' [!] Invalid response from the API')
                continue

            delegates = result['data']['governance']['delegates']
            chart_data = {
                'xAxis': {
                    'type': 'time',
                },
                'yAxis': {
                    'type': 'value',
                },
                'series': [],
            }
            for delegate in delegates:
                series_data = {
                    'name': delegate['account']['name'],
                    'picture': delegate['account']['picture'],
                    'address': delegate['account']['address'],
                    'ens': delegate['account']['ens'],
                    'bio': delegate['account']['bio'],
                    'twitter': delegate['account']['twitter'],
                    'email': delegate['account']['email'],
                    'tally_url': f"https://www.tally.xyz/profile/{delegate['account']['address']}?governanceId={self.governance_id}",
                    'type': 'line',
                    'data': [],
                }

                for change in delegate['participation']['weightChanges']:
                    # Append timestamp as X and newBalance as Y, normalized to K
                    series_data['data'].append({
                        'timestamp': change['timestamp'],
                        # rounding to 2 decimal places
                        'balance': self._scale_down(change['newBalance'])
                    })

                # Append series data to chart data
                chart_data['series'].append(series_data)

            # all_intervals[interval.value] = chart_data

            self.collection_refs['governance'].document(
                f'voting_power_chart_{interval.value.lower()}').set({'data': chart_data})

    class DelegateSortField(Enum):
        CREATED = 'CREATED'
        UPDATED = 'UPDATED'
        TOKENS_OWNED = 'TOKENS_OWNED'
        VOTING_WEIGHT = 'VOTING_WEIGHT'
        DELEGATIONS = 'DELEGATIONS'
        HAS_ENS = 'HAS_ENS'
        HAS_DELEGATE_STATEMENT = 'HAS_DELEGATE_STATEMENT'
        PROPOSALS_CREATED = 'PROPOSALS_CREATED'
        VOTES_CAST = 'VOTES_CAST'


    def _scale_down(self,value):
            return round(int(value) / 1e21, 2)
        

    def delegates(self, **kwargs):        
        # default voting_weight
        
        for order_by in self.DelegateSortField:
            query = """
                query Delegates($governanceId: AccountID!, $sort: DelegateSort, $pagination: Pagination) {
                governance(id: $governanceId) {
                    delegates(sort: $sort, pagination: $pagination) {
                        account {
							id
							name
							picture
							address
							bio
							email
							twitter
							ens
                        }
                        participation {
                            stats {
								tokenBalance
                                activeDelegationCount
                                createdProposalsCount
                                delegationCount
                                recentParticipationRate {
                                    recentProposalCount
                                    recentVoteCount
                                }
                                tokenBalance
                                voteCount
                                votingPower {
                                    in
                                    net
                                    out
                                }
                                weight {
                                    total
                                    owned
                                }
                                votes {
                                    total
                                }
                                delegations {
                                    total
                                }
                            }
                        }
                    }
                }
            }
            """

            variables = {
                "governanceId": self.governance_id,
                "pagination": {
                    "limit": 10,
                    "offset": 0
                },
                "sort": {
                    "field": order_by.value,
                    "order": "DESC"
                }

            }

            result = self.actor.governance_graphql_make_query(
                query, variables=variables)

            if not self.is_valid(result):
                logger.info(f' [!] Invalid response from the API')
                continue

            delegates = result['data']['governance']['delegates']

            flatted_delegates = []

            for delegate in delegates:
                delegate['participation']['stats']['tokenBalance'] = self._scale_down(
                    delegate['participation']['stats']['tokenBalance'])
                
                delegate['participation']['stats']['weight']['total'] = self._scale_down(
                    delegate['participation']['stats']['weight']['total'])
                
                delegate['participation']['stats']['weight']['owned'] = self._scale_down(
                    delegate['participation']['stats']['weight']['owned'])
                
                delegate['participation']['stats']['votingPower']['in'] = self._scale_down(
                    delegate['participation']['stats']['votingPower']['in'])
                
                delegate['participation']['stats']['votingPower']['net'] = self._scale_down(
                    delegate['participation']['stats']['votingPower']['net'])
                
                delegate['participation']['stats']['votingPower']['out'] = self._scale_down(
                    delegate['participation']['stats']['votingPower']['out'])
                
                delegate['account']['tally_url'] = f"https://www.tally.xyz/profile/{delegate['account']['address']}?governanceId={self.governance_id}"

                flatted_delegates.append(delegate)

            self.collection_refs['governance'].document(
                f'delegates_{order_by.value.lower()}').set({'data': flatted_delegates})


    def proposals(self, **kwargs):
        query = """
            query GovernanceProposals($sort: ProposalSort, $pagination: Pagination, $governanceIds: [AccountID!], $chainId: ChainID!, $proposerIds: [AccountID!], $votersPagination: Pagination, $includeVotes: Boolean!) {
                    proposals(
                        sort: $sort
                        pagination: $pagination
                        governanceIds: $governanceIds
                        proposerIds: $proposerIds
						chainId: $chainId
                    ) {
						createdTransaction {
							block {
								number
								timestamp
							}
						}
						description
						governor {
							quorum
						}
                        id
						eta
						executable {
							callDatas
							signatures
							targets
							values
						}
                        title
						
                        end {
                            timestamp
                        }
                        start{
                            timestamp
                        }
                        block {
                            timestamp
                            number
                        }
                        proposer {
							id
							name
							picture
							address
							bio
							email
							twitter
							ens
                        }
                        voteStats {
                            votes
                            weight
                            support
                            percent
                        }
                        statusChanges{
							type
							txHash
							transaction {
								block {
									timestamp
									number
								}
							}
						}
                        votes(pagination: $votersPagination) @include(if: $includeVotes) {
                            weight
							reason
							support
							transaction {
								block {
									number
									timestamp
								}
							}
							voter {
							id
							name
							picture
							address
							bio
							email
							twitter
							ens

							}
							weight
							
                        }
                    }
                }
        """

        variables = {"pagination": {"limit": 10, "offset": 0},
                    "sort": {"field": "START_BLOCK", "order": "DESC"},
                    "governanceIds": [self.governance_id],
                    "includeVotes": True,
                    "chainId": self.chain_id,
                    "votersPagination": {"limit": 10, "offset": 0}}


        result = self.actor.governance_graphql_make_query(
            query, variables=variables)
        
        if not self.is_valid(result):
            logger.info(f' [!] Invalid response from the API')
            return
        
        proposals = result['data']['proposals']

        flattened_proposals = []
        for proposal in proposals:
            # scale down all values

            proposal['id'] = int(proposal['id'])
            proposal['tally_url'] = f"https://www.tally.xyz/gov/{self.slug}/proposal/{proposal['id']}"
            for vote_stat in proposal['voteStats']: 
                vote_stat['weight'] = self._scale_down(
                    vote_stat['weight'])

            for vote in proposal['votes']:
                vote['weight'] = self._scale_down(
                    vote['weight'])
                vote['tally_url'] = f"https://www.tally.xyz/profile/{vote['voter']['address']}?governanceId={self.governance_id}"

            proposal['proposer']['tally_url'] = f"https://www.tally.xyz/profile/{proposal['proposer']['address']}?governanceId={self.governance_id}"

            flattened_proposals.append(proposal)
        
        self.collection_refs['governance'].document(
            'proposals').set({'data': flattened_proposals})

                
    def governance_info(self, **kwargs):
        query = """
        query GovernanceTrendingDelegates($governanceId: AccountID!) {
                    governance(id: $governanceId) {
                        active
                        chainId
                        contracts{
                            governor {
                                address
                            }
                            tokens{
                                address
                            }
                        }
                        proposalThreshold
                        timelockId
						kind
						tokens {
							decimals
							id
							name
							symbol
							type
							isIndexing
							lastIndexedBlock {
								timestamp
								number
							}
						}
                        stats {
                            proposals {
                                passed
                                failed
                                active
                                total
                            }
                            tokens{
                                voters
                                supply
                                owners
                                delegates {
                                    total
                                }
                                delegatedVotingPower
                            }
                        }
                        id
                        organization {
                            description
                            id
                            website
                            name
                            votingParameters {
                                bigVotingPeriod
                                quorum
                                votingPeriod
                            }
                            visual {
                                icon
								color
							}
							slug
                        }
                    }
                }
        """

        variables = {"governanceId": self.governance_id}

        result = self.actor.governance_graphql_make_query(

            query, variables=variables)
        
        if not self.is_valid(result):
            logger.info(f' [!] Invalid response from the API')
            return
        
        governance_info = result['data']['governance']

        governance_info['proposalThreshold'] = self._scale_down(
            governance_info['proposalThreshold'])
        
        governance_info['contracts']['timelock'] = {}
        governance_info['contracts']['timelock']['address'] = governance_info['timelockId'].split(':')[-1]
        
        governance_info['stats']['tokens']['supply'] = self._scale_down(
            governance_info['stats']['tokens']['supply'])
        

        governance_info['stats']['tokens']['delegatedVotingPower'] = self._scale_down(
            governance_info['stats']['tokens']['delegatedVotingPower'])
        
    
        governance_info['organization']['votingParameters']['quorum'] = self._scale_down(
            governance_info['organization']['votingParameters']['quorum'] )
        
        governance_info['organization']['tally_url'] = f"https://www.tally.xyz/gov/{self.slug}"

        self.collection_refs['governance'].document(
            'governance_info').set({'data': governance_info})
        
    
    def safes(self, **kwargs):
        data = self.actor.governance_rest_make_request(url = f"/safes?organizationId={self.organization_id}", max_page_fetch=1)

        if not self.is_valid(data):
            logger.info(f' [!] Invalid response from the API')
            return
        for safe in data['gnosisSafes']:
            safe['tally_url'] = f"https://www.tally.xyz/safe/{safe['id']}"
            for owner in safe['owners']:
                owner['tally_url'] = f"https://www.tally.xyz/profile/{owner['address']}?governanceId={self.governance_id}"

        self.collection_refs['governance'].document(
            'safes').set({'data': data})
        
