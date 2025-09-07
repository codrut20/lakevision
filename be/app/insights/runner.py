from typing import List, Dict, Any, Optional, Set
from .rules import Insight, ALL_RULES_OBJECT, InsightRun
from .utils import qualified_table_name, get_namespace_and_table_name
from app.storage import run_storage
from .job_schedule import JobSchedule

def execute_job(schedule: JobSchedule):
    """
    Takes a schedule object and executes the insight run.
    """
    print(f"Executing job for namespace '{schedule.namespace}'...")
    runner = InsightsRunner(lv)

    target = schedule.namespace
    if schedule.table_name:
        target += f".{schedule.table_name}"

    raw_insights = runner.run_for_target(target, rule_ids=schedule.rules_requested)

    records_to_save = []
    for insight in raw_insights:
        record = InsightRun(
            namespace=insight.namespace,
            table=insight.table,
            run_type='auto', # This is an automated run
            rule_id=insight.rule.id,
            rule_name=insight.rule.name,
            details=insight.to_dict()
        )
        records_to_save.append(record)
    
    if records_to_save:
        run_storage.save_many(records_to_save)

    print(f"Execution finished for schedule {schedule.schedule_id}. Found {len(records_to_save)} insights.")

class InsightsRunner:
    def __init__(self, lakeview):
        self.lakeview = lakeview

    def get_latest_run(self, table_identifier: str, page: int, size: int):
        """
        Fetches a paginated list of insight runs from storage.
        """
        namespace, table_name = get_namespace_and_table_name(table_identifier)
        run_storage.connect()
        criteria = {
            "namespace": namespace,
            "table_name": table_name
        }

        # 1. Get the total count of documents matching the criteria first
        total_count = run_storage.get_aggregate("COUNT", "*", criteria)
        
        # 2. Calculate skip and limit for pagination
        skip = (page - 1) * size
        
        # 3. Fetch the paginated slice of documents
        results = run_storage.get_by_attributes(criteria, skip=skip, limit=size)
        
        run_storage.disconnect()
        
        # 4. Return both the items for the page and the total count
        return {
            "items": results,
            "total": total_count
        }

    def run_for_table(self, table_identifier, rule_ids: List[str] = None) -> List[Insight]:
        table = self.lakeview.load_table(table_identifier)
    
        all_valid_ids: Set[str] = {rule.id for rule in ALL_RULES_OBJECT}
        
        ids_to_run: Set[str]
        
        if rule_ids is None:
            ids_to_run = all_valid_ids
        else:
            provided_ids = set(rule_ids)
            invalid_ids = provided_ids - all_valid_ids
            
            if invalid_ids:
                raise ValueError(f"Invalid rule IDs provided: {', '.join(sorted(invalid_ids))}")
        
            ids_to_run = provided_ids

        namespace, table_name = get_namespace_and_table_name(table_identifier)
        print("here")

        run_result = [
            insight
            for rule in ALL_RULES_OBJECT
            if rule.id in ids_to_run and (insight := rule.method(table))
        ]
        print(run_result)
        run = InsightRun(
            namespace=namespace,
            table_name=table_name,
            run_type='manual',
            results=run_result,
            rules_requested=list(ids_to_run)
        )
        run_storage.connect()
        run_storage.save(run)
        run_storage.disconnect()

        return run_result

    def run_for_namespace(self, namespace: str, rule_ids: List[str] = None, recursive: bool = True) -> Dict[str, List[Insight]]:
        tables = self.lakeview.get_tables(namespace)
        results = {}
        for t_ident in tables:
            qualified = qualified_table_name(t_ident)
            results[qualified] = self.run_for_table(t_ident, rule_ids)
        if recursive:
            nested_namespaces = self.lakeview._get_nested_namespaces(namespace)
            for ns in nested_namespaces:
                ns_str = ".".join(ns)
                results.update(self.run_for_namespace(ns_str, rule_ids, recursive=False))
        return results

    def run_for_lakehouse(self, rule_ids: List[str] = None) -> Dict[str, List[Insight]]:
        namespaces = self.lakeview.get_namespaces(include_nested=False)
        results = {}
        for ns in namespaces:
            ns_str = ".".join(ns) if isinstance(ns, (tuple, list)) else str(ns)
            results.update(self.run_for_namespace(ns_str, rule_ids, recursive=True))
        return results
