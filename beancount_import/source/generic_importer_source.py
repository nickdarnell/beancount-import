"""This module implements a Source Subclass for wrapping
`beancount.ingest.importer.ImporterProtocol` subclasses importers.
The importers are considered athoritative of the account they represent.

The Transaction.narration set by each importer is copied to Posting.meta[source_desc]
This helps in predicting postings for similar transaction while allowing the
user to change the Transaction description and payee from UI
(see readme.md for more on source_desc)
This `source_desc` meta is also used for check cleared postings and should not be
changed manually

Author: Sufiyan Adhikari(github.com/dumbPy)
"""

import os
import datetime
from glob import glob
from collections import OrderedDict
import itertools
from typing import Hashable, List, Dict, Optional

from beancount.core.data import Balance, Transaction, Posting,  Directive
from beancount.core.amount import Amount
from beancount.core.convert import get_weight
from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.cache import get_file
from beancount.parser.booking_full import convert_costspec_to_cost

from ..matching import FIXME_ACCOUNT, SimpleInventory
from . import ImportResult, SourceResults
from ..journal_editor import JournalEditor
from .description_based_source import DescriptionBasedSource, get_pending_and_invalid_entries, get_account_mapping, get_posting_source_descs


class ImporterSource(DescriptionBasedSource):
    def __init__(self,
                 directory: str,
                 account: str,
                 importer: ImporterProtocol,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = os.path.expanduser(directory)
        self.importer = importer
        self.account = account

        # get _FileMemo object for each file
        files = [get_file(os.path.abspath(f)) for f in
                    filter(os.path.isfile,
                 glob(os.path.join(directory, '**', '*'), recursive=True)
                           )
        ]
        # filter the valid files for this importer
        self.files = [f for f in files if self.importer.identify(f)]

    @property
    def name(self) -> str:
        return self.importer.name()

    def prepare(self, journal: 'JournalEditor', results: SourceResults) -> None:
        results.add_account(self.account)

        entries = OrderedDict() #type: Dict[Hashable, List[Directive]]
        for f in self.files:
            f_entries = self.importer.extract(f, existing_entries=journal.entries)
            # collect  all entries in current statement, grouped by hash
            hashed_entries = OrderedDict() #type: Dict[Hashable, Directive]
            for entry in f_entries:
                key_ = self._get_key_from_imported_entry(entry)
                self._add_description(entry)
                hashed_entries.setdefault(key_, []).append(entry)
            # deduplicate across statements
            for key_ in hashed_entries:
                # skip the existing entries from other statements. add remaining
                if not key_ in entries:
                    n = 0
                else:
                    n = len(entries[key_])
                entries.setdefault(key_, []).extend(hashed_entries[key_][n:])

        raw_entries = list(itertools.chain.from_iterable(entries.values()))

        get_pending_and_invalid_entries(
            raw_entries=raw_entries,
            journal_entries=journal.all_entries,
            account_set=self.get_source_accounts(journal, raw_entries),
            get_key_from_posting=self._get_key_from_posting,
            get_key_from_raw_entry=self._get_key_from_imported_entry,
            make_import_result=self._make_import_result,
            results=results)
        
    def get_source_accounts(self, journal: 'JournalEditor', raw_entries):
        accounts = set([self.account])
    
        for raw_entry in raw_entries:
            if not isinstance(raw_entry, Transaction):
                continue
        
            for posting in raw_entry.postings:
                if posting.account.startswith(self.account):
                    accounts.add(posting.account)
        
        return accounts

    def _add_description(self, entry: Transaction):
        if not isinstance(entry, Transaction): return None
        postings = entry.postings #type: List[Posting]
        to_mutate = []
        for i, posting in enumerate(postings):
            if posting.account != self.account: continue
            if isinstance(posting.meta, dict):
                posting.meta["source_desc"] = entry.narration
                posting.meta["date"] = entry.date
                break
            else:
                to_mutate.append(i)
                break
        for i in to_mutate:
            p = postings.pop(i)
            p = Posting(p.account, p.units, p.cost, p.price, p.flag,
                        {"source_desc":entry.narration, "date": entry.date})
            postings.insert(i, p)

    def _get_source_posting(self, entry:Transaction) -> Optional[Posting]:
        for posting in entry.postings:
            if posting.account.startswith(self.account):
                return posting
        return None
    
    def _get_key_from_posting(self, entry: Transaction, posting: Posting,
                                source_postings: List[Posting], source_desc: str,
                                posting_date: datetime.date):
        del entry
        del source_postings
        return (posting.account, posting_date, posting.units, source_desc)

    def _get_key_from_imported_entry(self, entry:Directive) -> Hashable:
        if isinstance(entry, Balance):
            return (entry.account, entry.date, entry.amount)
        if not isinstance(entry, Transaction):
            raise ValueError("currently, ImporterSource only supports Transaction and Balance Directive. Got entry {}".format(entry))
        source_posting = self._get_source_posting(entry)
        if source_posting is None:
            raise ValueError("entry {} has no postings for account: {}".format(entry, self.account))
        for source_posting_desc, source_posting_date in get_posting_source_descs(source_posting):
            return (source_posting.account,
                    source_posting_date,
                    source_posting.units,
                    source_posting_desc)
    
    def _balance_amounts_if_needed(self, txn:Transaction)-> None:
        # Only try and balance transactions with less than 2 postings.
        if len(txn.postings) < 2:
            balance_amounts(txn)

    def _make_import_result(self, imported_entry:Directive):
        if isinstance(imported_entry, Transaction):
            self._balance_amounts_if_needed(imported_entry)
        
        result = ImportResult(
            date=imported_entry.date, info=get_info(imported_entry), entries=[imported_entry])
        
        # delete filename since it is used by beancount-import to determine if the
        # entry is from journal.
        imported_entry.meta.pop('filename')
        return result


def get_info(raw_entry: Directive) -> dict:
    if raw_entry.meta["filename"].endswith(".beancount"):
        ftype = "text/plain"
    else:
        ftype = get_file(raw_entry.meta['filename']).mimetype()
    return dict(
        type=ftype,
        filename=raw_entry.meta['filename'],
        line=raw_entry.meta['lineno'],
    )

def balance_amounts(txn:Transaction)-> None:
    """Add FIXME account for the remaing amount to balance accounts"""
    inventory = SimpleInventory()
    for posting in txn.postings:
        inventory += get_weight(convert_costspec_to_cost(posting))
    for currency in inventory:
        txn.postings.append(
            Posting(
                account=FIXME_ACCOUNT,
                units=Amount(currency=currency, number=-inventory[currency]),
                cost=None,
                price=None,
                flag=None,
                meta={},
            ))


def load(spec, log_status):
    return ImporterSource(log_status=log_status, **spec)
