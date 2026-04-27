"""Error-mapping contract for parsimony-bdp."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_bdp import BdpFetchParams, bdp_fetch


class TestBdpFetchErrorMapping(ErrorMappingSuite):
    connector = bdp_fetch
    params = BdpFetchParams(domain_id=11, dataset_id="ABC")
    route_url = "https://bpstat.bportugal.pt/data/v1/domains/11/datasets/ABC/"
    env_key = None
