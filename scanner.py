from collections import defaultdict
import signal
import json
from typing import Set, Dict, List, Tuple

import web3
from web3._utils.events import get_event_data
from web3._utils.filters import construct_event_filter_params
from web3.exceptions import LogTopicError


class ContractStreamer:
    def __init__(self, abi: Dict, w3: web3.Web3):
        self.w3 = w3
        self.contract = self.w3.eth.contract(abi=abi)
        '''
        ETH smart contract is defined by it's ABI, In json file there is a required abi for ERC721 type of contracts
        https://ethereum.org/ru/developers/docs/standards/tokens/erc-721/
        '''

    def fetch_addresses(self, from_block: int, to_block: int) -> List[Tuple[int, List[str]]]:
        results = defaultdict(set)
        '''
        Here I scan for the logs in the blocks, seeking for events from the ERC721 contracts
        '''
        for event_type in self.contract.events:
            event_abi = event_type._get_event_abi()

            _, event_filter_params = construct_event_filter_params(
                event_abi, self.w3.codec, fromBlock=from_block, toBlock=to_block,
            )

            logs = self.w3.eth.get_logs(event_filter_params)
            for log in logs:
                try:
                    _ = get_event_data(self.w3.codec, event_abi, log)
                except LogTopicError as e:
                    continue

                results[log.blockNumber].add(log.address)

        output: List[Tuple[int, Set[str]]] = []
        for block in range(from_block, to_block + 1):
            output.append((block, list(results.get(block, set()))))

        return output

    def stream_addresses(self, from_block: int):
        step = 9  # I found that with default provider I could get up to 9 blocks at time
        cur_block = from_block
        max_block = self.w3.eth.get_block_number()  # No reason to iterate after newest block number
        while True:
            if cur_block > max_block:
                break
            to_block = cur_block + step
            for record in self.fetch_addresses(cur_block, to_block):
                yield record
            cur_block = to_block


def handler(signum, frame):
    exit(1)  # Just handy to supress Ctrl-C output

if __name__ == '__main__':
    signal.signal(signal.SIGINT, handler)

    provider_url = 'https://eth-mainnet.g.alchemy.com/v2/Z7WMHhhmjHGNI7iJS-Istbek1wg-a7ZI'
    with open('abi.json', 'r') as f:
        abi = json.load(f)

    w3 = web3.Web3(web3.HTTPProvider(provider_url))

    if w3.isConnected():
        block_number = None
        while not block_number:
            print('Provide a starting block number')
            try:
                block_number = int(input())
            except ValueError:
                print('Block number should be a valid integer')
        
        contract_streamer = ContractStreamer(abi=abi, w3=w3)
        for record in contract_streamer.stream_addresses(block_number):
            if not record[1]:  #skip blocks w/o ERC721 addresses
                continue
            print(f'block #{record[0]}')
            for addr in record[1]:
                print(addr)
    else:
        print(f'Provider {provider_url} could not connect')
