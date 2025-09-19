<script>
	// --- EXISTING IMPORTS ---
	import {
		Tile,
		ExpandableTile,
		Dropdown,
		Content,
		Tabs,
		Tab,
		TabContent,
		Grid,
		Row,
		Column,
		CopyButton,
		ToastNotification,
		DataTableSkeleton,
		Loading,
		Button,
		ButtonSet,
		Pagination,
		Modal,
		TextInput,
		Checkbox,
		FormGroup,
		Select,
		SelectItem,
		Tag
	} from 'carbon-components-svelte';
	import {
		Run,
		Calendar,
        Renew,
        CheckmarkFilled,
        WarningAltFilled
	} from 'carbon-icons-svelte';
	import { selectedNamespce, selectedTable, sample_limit } from '$lib/stores';
	import JsonTable from '../lib/components/JsonTable.svelte';
	import { BarChartSimple } from '@carbon/charts-svelte';
	import '@carbon/charts-svelte/styles.css';
	import options from './options';
	import VirtualTable from '../lib/components/VirtTable3.svelte';
	import { goto } from '$app/navigation';
	import QueryRunner from '../lib/components/QueryRunner.svelte';
	import { get } from 'svelte/store';
	import { onMount } from 'svelte';

	let namespace;
	let table;
	let ns_props;
	let tab_props;
	let error = '';
	let url = '';
	let pageSessionId = Date.now().toString(36) + Math.random().toString(36).substring(2);

	$: {
		selectedNamespce.subscribe((value) => {
			namespace = value;
		});
		selectedTable.subscribe((value) => {
			table = value;
		});
		if (namespace) ns_props = get_namespace_special_properties(namespace);
		if (namespace && table) tab_props = get_table_special_properties(namespace + '.' + table);
	}

	// --- EXISTING STATE FOR OTHER TABS ---
	let partitions = [];
	let partitions_loading = false;
	let snapshots = [];
	let snapshots_loading = false;
	let sample_data = [];
	let sample_data_loading = false;
	let schema = [];
	let schema_loading = false;
	let summary = [];
	let summary_loading = false;
	let partition_specs = [];
	let partition_specs_loading = false;
	let sort_order = [];
	let sort_order_loading = false;
	let properties = [];
	let properties_loading = false;
	let data_change = [];
	let data_change_loading = false;
	let access_allowed = true;

	// --- STATE & LOGIC FOR INSIGHTS TAB ---

	let insights_loading = true;
	/** @type {any[]} */
	let insightRuns = [];
	/** @type {any[]} */
	let allRules = [];
    /** @type {Map<string, string>} */
    let ruleIdToNameMap = new Map();
	let openRunModal = false;
	let openScheduleModal = false;
    let expandedRules = {};

	let currentPage = 1;
	const pageSizes = [5, 10, 25, 50];
	let pageSize = 5;
	let totalItems = 0;

	const virtualTableColumns = {
		'Run Type': '',
		'Timestamp': '',
		'Rules & Results': ''
	};
    
    let columnWidths = {
        'Run Type': 150,
        'Timestamp': 250,
        'Rules & Results': 600
    };

    // Calculate height for each individual row based on its content
    let insightRowHeights = [];
    $: {
        insightRowHeights = insightRuns.map(run => {
            const baseHeight = 40; // Base padding for the row
            const heightPerRule = 28; // Estimated height for each rule name line
            const heightPerExpandedMessage = 65; // Estimated height for an expanded message card

            const rulesListHeight = run.rules_requested.length * heightPerRule;
            const messagesHeight = run.results.length * heightPerExpandedMessage;
            
            // A row's height is the space for the rules list plus the space for all its potential messages.
            return Math.max(baseHeight, rulesListHeight + messagesHeight);
        });
    }

	let manualRunData = { namespace: '', tableName: '', rules_requested: [] };
    let manualRunSelectAll = false;
    let manualRunIndeterminate = false;
	let scheduleRunData = { namespace: '', tableName: '', rules_requested: [], cron_schedule: '0 0 * * 0', frequency: 'weekly', created_by: 'testuser' };
    let scheduleRunSelectAll = false;
    let scheduleRunIndeterminate = false;

    $: if (namespace && table) {
        manualRunData.namespace = namespace;
        manualRunData.tableName = table;
        scheduleRunData.namespace = namespace;
        scheduleRunData.tableName = table;
    }

    $: {
        if (allRules.length > 0) {
            const allSelectedManual = manualRunData.rules_requested.length === allRules.length;
            const someSelectedManual = manualRunData.rules_requested.length > 0 && !allSelectedManual;
            manualRunSelectAll = allSelectedManual;
            manualRunIndeterminate = someSelectedManual;
			const allSelectedSchedule = scheduleRunData.rules_requested.length === allRules.length;
            const someSelectedSchedule = scheduleRunData.rules_requested.length > 0 && !allSelectedSchedule;
            scheduleRunSelectAll = allSelectedSchedule;
            scheduleRunIndeterminate = someSelectedSchedule;
        }
    }

    function toggleSelectAllManual(event) { if (event.currentTarget.checked) { manualRunData.rules_requested = allRules.map(rule => rule.id); } else { manualRunData.rules_requested = []; } }
    function toggleSelectAllSchedule(event) { if (event.currentTarget.checked) { scheduleRunData.rules_requested = allRules.map(rule => rule.id); } else { scheduleRunData.rules_requested = []; } }

	function handleFrequencyChange() {
		const freq = scheduleRunData.frequency;
		switch (freq) {
			case 'weekly': scheduleRunData.cron_schedule = '0 0 * * 0'; break;
			case 'biweekly': scheduleRunData.cron_schedule = '0 0 1,15 * *'; break;
			case 'monthly': scheduleRunData.cron_schedule = '0 0 1 * *'; break;
			case 'bimonthly': scheduleRunData.cron_schedule = '0 0 1 */2 *'; break;
		}
	}

	async function fetchTableInsights(page = 1, size = 10) {
		if (!namespace || !table) return;
		insights_loading = true;
		try {
			const response = await fetch(`/api/tables/${namespace}.${table}/insights/latest?page=${page}&size=${size}`);
			if (!response.ok) throw new Error('Failed to fetch insights');
			
			const responseData = await response.json();
			let data;

			if (Array.isArray(responseData)) {
				console.warn("API is not paginated yet. Displaying all results.");
				data = responseData;
				totalItems = data.length;
			} else {
				data = responseData.items || [];
				totalItems = responseData.total || 0;
			}
			
			data.sort((a, b) => new Date(b.run_timestamp) - new Date(a.run_timestamp));
			insightRuns = data;
		} catch (error) {
			console.error('Error fetching table insights:', error);
			alert('Could not refresh insights data.');
		} finally {
			insights_loading = false;
		}
	}
	
	async function onPaginationChange(event) {
		const { page: newPage, pageSize: newPageSize } = event.detail;
		const pageChanged = newPage !== currentPage;
		const pageSizeChanged = newPageSize !== pageSize;
		if (pageSizeChanged) {
			currentPage = 1;
			pageSize = newPageSize;
			await fetchTableInsights(currentPage, pageSize);
		} else if (pageChanged) {
			currentPage = newPage;
			await fetchTableInsights(currentPage, pageSize);
		}
	}

	async function fetchAllRules() {
		try {
			const response = await fetch('/api/lakehouse/insights/rules');
			if (!response.ok) throw new Error('Failed to fetch rules');
			allRules = await response.json();
            ruleIdToNameMap = new Map(allRules.map(rule => [rule.id, rule.name]));
		} catch (error) {
			console.error('Error fetching rules:', error);
		}
	}

	async function handleManualRunSubmit() {
		if (manualRunData.rules_requested.length === 0) { alert('Please select at least one rule to run.'); return; }
		try {
			const response = await fetch('/api/start-run', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ namespace: manualRunData.namespace, table_name: manualRunData.tableName, rules_requested: manualRunData.rules_requested }) });
			if (response.status !== 202) throw new Error('Failed to start run');
			const result = await response.json();
			alert(`Run started successfully! Run ID: ${result.run_id}`);
			openRunModal = false;
			setTimeout(() => fetchTableInsights(currentPage, pageSize), 3000);
		} catch (error) {
			console.error('Error starting manual run:', error);
			alert(`Error: ${error.message}`);
		}
	}

	async function handleScheduleSubmit() {
		if (scheduleRunData.rules_requested.length === 0) { alert('Please select at least one rule for the schedule.'); return; }
		try {
			const response = await fetch('/api/schedules', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ namespace: scheduleRunData.namespace, table_name: scheduleRunData.tableName || null, rules_requested: scheduleRunData.rules_requested, cron_schedule: scheduleRunData.cron_schedule, created_by: scheduleRunData.created_by }) });
			if (response.status !== 201) throw new Error('Failed to create schedule');
			const result = await response.json();
			alert(`Schedule created successfully! Schedule ID: ${result.id}`);
			openScheduleModal = false;
			setTimeout(() => fetchTableInsights(currentPage, pageSize), 3000);
		} catch (error) {
			console.error('Error creating schedule:', error);
			alert(`Error: ${error.message}`);
		}
	}

	let lastSampleLimit = null;
	let sampleLimits = [ { id: 10, text: '10' }, { id: 50, text: '50' }, { id: 100, text: '100' }, { id: 500, text: '500' }, { id: 1000, text: '1000' }, { id: 10000, text: '10000' } ];
	if (!sampleLimits.find((item) => item.id === get(sample_limit))) { sampleLimits.push({ id: get(sample_limit), text: get(sample_limit).toString() }); }
	sampleLimits = sampleLimits.sort((a, b) => { if (a.id < b.id) return -1; if (a.id > b.id) return 1; return 0; });
	let selectedLimit = sampleLimits.find((item) => item.id === get(sample_limit));
	sample_limit.set(selectedLimit.id);
	selectedLimit = sampleLimits.find((item) => item.id === selectedLimit.id);
	$: if (selectedLimit) { sample_limit.set(selectedLimit.id); }
	async function get_namespace_special_properties(namespace_name) { ns_props = await fetch(`/api/namespaces/${namespace_name}/special-properties`, { headers: { 'Content-Type': 'application/json', 'X-Page-Session-ID': pageSessionId } }).then((res) => res.json()); return ns_props; }
	async function get_table_special_properties(table_id) { tab_props = await fetch(`/api/tables/${table_id}/special-properties`, { headers: { 'Content-Type': 'application/json', 'X-Page-Session-ID': pageSessionId } }).then((res) => res.json()); return tab_props; }
	async function get_data(table_id, feature) { let loading = true; if (!table_id || table_id == null || table_id == '.' || !table) { loading = false; return; } try { const res = await fetch(`/api/tables/${table_id}/${feature}`, { headers: { 'Content-Type': 'application/json', 'X-Page-Session-ID': pageSessionId } }); const statusCode = res.status; if (res.ok) { const data = await res.json(); return data; } else if (statusCode == 403) { console.log('No Access'); error = 'No Access'; access_allowed = false; return error; } else if (res.status === 401) { goto('/api/login?namespace=' + namespace + '&table=' + table + '&sample_limit=' + $sample_limit); } else { console.error('Failed to fetch data:', res.statusText); error = res.statusText; } } finally { loading = false; } }
	let selected = 0;
	$: reset(table);
	let callOnce = 0;
	async function fetchSampleData() { sample_data_loading = true; try { sample_data = await get_data(namespace + '.' + table, `sample?sample_limit=${$sample_limit}`); lastSampleLimit = $sample_limit; } catch (err) { error = err.message; } finally { sample_data_loading = false; } }
	$: if (selected === 3 && $sample_limit !== lastSampleLimit && !sample_data_loading && namespace && table) { fetchSampleData(); }
	
    $: (async () => {
		if (table === '') return;
		try { if (selected == 0) { if (callOnce > 0) { callOnce = 0; return; } callOnce++; summary_loading = true; summary = await get_data(namespace + '.' + table, 'summary'); if (tab_props && 'restricted' in tab_props) { summary['Restricted'] = tab_props['restricted']; } summary_loading = false; } } catch (err) { error = err.message; summary_loading = false; } 
		try { if (selected == 0) { properties_loading = true; properties = await get_data(namespace + '.' + table, 'properties'); properties_loading = false; } } catch (err) { error = err.message; properties_loading = false; } 
		try { if (selected == 0) { schema_loading = true; schema = await get_data(namespace + '.' + table, 'schema'); schema_loading = false; } } catch (err) { error = err.message; schema_loading = false; } 
		try { if (selected == 0) { partition_specs_loading = true; partition_specs = await get_data(namespace + '.' + table, 'partition-specs'); partition_specs_loading = false; } } catch (err) { error = err.message; partition_specs_loading = false; } 
		try { if (selected == 0) { sort_order_loading = true; sort_order = await get_data(namespace + '.' + table, 'sort-order'); sort_order_loading = false; } } catch (err) { error = err.message; sort_order_loading = false; } 
		try { if (selected == 1 && partitions.length == 0 && !partitions_loading) { partitions_loading = true; partitions = await get_data(namespace + '.' + table, 'partitions'); partitions_loading = false; } } catch (err) { error = err.message; partitions_loading = false; } 
		try { if (selected == 2 && snapshots.length == 0) { snapshots_loading = true; snapshots = await get_data(namespace + '.' + table, 'snapshots'); snapshots_loading = false; } } catch (err) { error = err.message; snapshots_loading = false; } 
		try { if (selected == 3 && sample_data.length == 0 && !sample_data_loading) { fetchSampleData(); } } catch (err) { error = err.message; sample_data_loading = false; }
	})();

	$: {
		if (selected === 5 && table) {
			if (insightRuns.length === 0 && totalItems === 0) {
				fetchTableInsights(currentPage, pageSize);
				if (allRules.length === 0) {
					fetchAllRules();
				}
			}
		}
	}

	function set_copy_url() { url = window.location.origin; url = url + '/?namespace=' + namespace + '&table=' + table + '&sample_limit=' + $sample_limit; }
	function reset(table) {
		lastSampleLimit = null; partitions = []; snapshots = []; sample_data = []; data_change = []; insightRuns = []; allRules = []; currentPage = 1; totalItems = 0;
		partitions_loading = false; sort_order_loading = false; snapshots_loading = false; sample_data_loading = false; data_change_loading = false; properties_loading = false; partition_specs_loading = false; schema_loading = false; summary_loading = false; schema = []; summary = []; partition_specs = []; properties = []; sort_order = []; 
	}
</script>

<Content>
	<Tile>
		<div class="tile-header">
			<div class="tile-content">
				<dl class="namespace-table-list">
					<dt>Namespace</dt>
					<dd>{namespace}</dd>
					<dt>Table</dt>
					<dd>{table}</dd>
				</dl>
			</div>
			<div class="copy-button-container">
				<CopyButton text={url} on:click={set_copy_url} iconDescription="Copy table link" feedback="Table link copied" />
			</div>
		</div>
	</Tile>
	<br />
	<Tabs bind:selected>
		<Tab label="Summary" />
		<Tab label="Partitions" />
		<Tab label="Snapshots" />
		<Tab label="Sample Data" />
		<Tab label="SQL" />
		<Tab label="Insights" />

		<svelte:fragment slot="content">
			<TabContent><br/><Grid><Row><Column aspectRatio="2x1"><h5>Summary</h5>{#if !summary_loading && properties}<JsonTable jsonData={summary} orient="kv" />{:else}<Loading withOverlay={false} small />{/if}</Column><Column aspectRatio="2x1"><h5>Schema</h5>{#if !schema_loading && schema.length > 0}<VirtualTable data={schema} columns={schema[0]} rowHeight={37} tableHeight={360} defaultColumnWidth={121}/>{/if}</Column></Row><Row><Column aspectRatio="2x1"><br /><br /><h5>Properties</h5>{#if !properties_loading && properties}<JsonTable jsonData={properties} orient="kv" />{/if}</Column><Column aspectRatio="2x1"><br /><br /><h5>Partition Specs</h5>{#if !partition_specs_loading && partition_specs}<JsonTable jsonData={partition_specs} orient="table" />{/if}<br /><h5>Sort Order</h5>{#if !sort_order_loading && sort_order}<JsonTable jsonData={sort_order} orient="table" />{/if}</Column></Row></Grid>{#if ns_props}<ExpandableTile light><div slot="below">{ns_props}</div></ExpandableTile>{/if}</TabContent>
			<TabContent><br/>{#if partitions_loading}<Loading withOverlay={false} small />{:else if !access_allowed}<ToastNotification hideCloseButton title="No Access" subtitle="You don't have access to the table data"></ToastNotification>{:else if partitions.length > 0}<VirtualTable data={partitions} columns={partitions[0]} rowHeight={35} enableSearch=true/><br />Total items: {partitions.length}{/if}</TabContent>
			<TabContent><br/>{#if snapshots_loading}<Loading withOverlay={false} small />{:else if snapshots.length > 0}<VirtualTable data={snapshots} columns={snapshots[0]} rowHeight={35} enableSearch=true/><br />Total items: {snapshots.length}{:else}No data{/if}</TabContent>
			<TabContent><br/><div class="sample-lable">Select # of rows to sample</div><Dropdown inline items={sampleLimits} bind:selectedId={selectedLimit.id} label="Sample Limit" titleText="Sample Limit" on:select={(e) => { selectedLimit = e.detail.selectedItem; sample_limit.set(selectedLimit.id); fetchSampleData(); }} />{#if sample_data_loading}<Loading withOverlay={false} small />{:else if !access_allowed}<ToastNotification hideCloseButton title="No Access" subtitle="You don't have access to the table data"></ToastNotification>{:else if sample_data.length > 0}<VirtualTable data={sample_data} columns={sample_data[0]} rowHeight={35} tableHeight={sample_data.length>13?500:(sample_data.length+1)*35} enableSearch=true/><br />Sample items: {sample_data.length}{/if}</TabContent>
			<TabContent><br/><QueryRunner tableName={namespace + '.' + table} {pageSessionId} /></TabContent>

			<TabContent>
                <br>
				<ButtonSet>
					<Button icon={Run} on:click={() => (openRunModal = true)}>Run Insights</Button>
					<Button icon={Calendar} kind="secondary" on:click={() => (openScheduleModal = true)}>Schedule Run</Button>
					<Button
						kind="ghost"
						hasIconOnly
						class="cds--btn--icon-only"
						icon={Renew}
						iconDescription="Refresh Data"
						tooltipPosition="right"
						on:click={() => fetchTableInsights(currentPage, pageSize)}
					/>
				</ButtonSet>
				<div style="margin-top: 1.5rem;">
					{#if insights_loading}
						<div style="height: 500px;"><DataTableSkeleton rowCount={5} columnCount={3} /></div>
					{:else if insightRuns.length === 0}
						<p>No insight runs found. Click "Run Insights" to start a new analysis.</p>
					{:else}
						<div class="insights-virtual-table-container">
							<VirtualTable 
								data={insightRuns} 
								columns={virtualTableColumns} 
								tableHeight={500}
								rowHeights={insightRowHeights}
								bind:columnWidths
							>
								<div slot="cell" let:row let:columnKey>
									{#if columnKey === 'Run Type'}
										<div class="cell-padding">
											<Tag type={row.run_type === 'manual' ? 'cyan' : 'green'} title={row.run_type}>{row.run_type}</Tag>
										</div>
									{:else if columnKey === 'Timestamp'}
										<div class="cell-padding">
											{new Date(row.run_timestamp).toLocaleString()}
										</div>
									{:else if columnKey === 'Rules & Results'}
										<div class="rules-cell-container">
											{#each row.rules_requested as ruleId}
												{@const hasResults = row.results.some(result => result.code === ruleId)}
												{@const compositeKey = `${row.id}-${ruleId}`}
												{@const ruleResults = row.results.filter(r => r.code === ruleId)}

												<div 
													class="rule-item" 
													class:clickable={hasResults} 
													on:click={() => { 
														if(hasResults) {
															expandedRules[compositeKey] = !expandedRules[compositeKey];
															expandedRules = expandedRules;
														}
													}}
													role={hasResults ? 'button' : 'listitem'}
													tabindex={hasResults ? 0 : -1}
													on:keydown={(e) => { if(e.key === 'Enter' && hasResults) e.currentTarget.click() }}
												>
													<div class="rule-item-header">
														{#if hasResults}
															<WarningAltFilled size={16} style="color: var(--cds-support-03, #ff832b);" />
														{:else}
															<CheckmarkFilled size={16} style="color: var(--cds-support-02, #24a148);" />
														{/if}
														<span>{ruleIdToNameMap.get(ruleId) || ruleId}</span>
													</div>
													
													{#if expandedRules[compositeKey]}
														<div class="rule-details">
															{#each ruleResults as result}
																<div class="message-card">
																	<p><strong>Message:</strong> {result.message}</p>
																	<p><strong>Suggested Action:</strong> {result.suggested_action}</p>
																</div>
															{/each}
														</div>
													{/if}
												</div>
											{/each}
										</div>
									{/if}
								</div>
							</VirtualTable>
						</div>
						{#if totalItems > pageSize}
							<Pagination
								page={currentPage}
								pageSize={pageSize}
								totalItems={totalItems}
								pageSizes={pageSizes}
								on:change={onPaginationChange}
							/>
						{/if}
					{/if}
				</div>
			</TabContent>
		</svelte:fragment>
	</Tabs>
</Content>

<Modal bind:open={openRunModal} modalHeading="Run New Insight" primaryButtonText="Start Run" secondaryButtonText="Cancel" on:submit={handleManualRunSubmit} on:click:button--secondary={() => openRunModal = false}>
    <div class="readonly-field"><span class="readonly-label">Namespace</span><TextInput value={manualRunData.namespace} readOnly /></div>
    <div class="readonly-field"><span class="readonly-label">Table Name</span><TextInput value={manualRunData.tableName} readOnly /></div>
    <hr class="modal-divider"/>
	<FormGroup legendText="Rules to Run"><Checkbox labelText="Select All" checked={manualRunSelectAll} indeterminate={manualRunIndeterminate} on:change={toggleSelectAllManual} /><hr class="modal-divider-light"/>
		{#if allRules.length > 0}
			<div class="rules-grid">
                {#each allRules as rule}
                    <Checkbox labelText={rule.name} value={rule.id} bind:group={manualRunData.rules_requested} />
                {/each}
            </div>
		{:else}
			<p>Loading rules...</p>
		{/if}
	</FormGroup>
</Modal>
<Modal bind:open={openScheduleModal} modalHeading="Schedule New Insight Run" primaryButtonText="Create Schedule" secondaryButtonText="Cancel" on:submit={handleScheduleSubmit} on:click:button--secondary={() => openScheduleModal = false}>
	<div class="readonly-field"><span class="readonly-label">Namespace</span><TextInput value={scheduleRunData.namespace} readOnly /></div>
    <div class="readonly-field"><span class="readonly-label">Table Name</span><TextInput value={scheduleRunData.tableName} readOnly /></div>
    <hr class="modal-divider"/>
	<FormGroup legendText="Rules to Schedule"><Checkbox labelText="Select All" checked={scheduleRunSelectAll} indeterminate={scheduleRunIndeterminate} on:change={toggleSelectAllSchedule} /><hr class="modal-divider-light"/>
		<div class="rules-grid">
            {#each allRules as rule}
                <Checkbox labelText={rule.name} value={rule.id} bind:group={scheduleRunData.rules_requested} />
            {/each}
        </div>
	</FormGroup>
    <hr class="modal-divider"/>
	<FormGroup legendText="Frequency">
		<Select bind:selected={scheduleRunData.frequency} on:change={handleFrequencyChange}>
			<SelectItem value="weekly" text="Weekly" />
			<SelectItem value="biweekly" text="Biweekly (1st and 15th)" />
			<SelectItem value="monthly" text="Monthly (1st)" />
			<SelectItem value="bimonthly" text="Bimonthly (1st of even months)" />
			<SelectItem value="custom" text="Custom" />
		</Select>
	</FormGroup>
    <FormGroup legendText="Cron Schedule">
        <TextInput bind:value={scheduleRunData.cron_schedule} required readonly={scheduleRunData.frequency !== 'custom'} helperText="Format: minute hour day(month) month day(week)" />
    </FormGroup>
</Modal>

<style>
	/* --- Existing Styles --- */
	.sample-lable { margin-bottom: 20px; }
	.tile-content { display: flex; flex-direction: column; gap: 10px; }
	.namespace-table-list { display: grid; grid-template-columns: auto 1fr; gap: 25px; margin: 0; font-size: 1.3em; }
	dt { font-weight: bold; }
	dd { margin: 0; }
	.copy-button-container { align-items: end; display: flex; justify-content: flex-end; }
	.tile-header { display: flex; justify-content: space-between; }
    .readonly-field { display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; }
    .readonly-label { flex-basis: 120px; flex-shrink: 0; font-weight: bold; }
    .rules-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 0.5rem 1rem; }
    .modal-divider { margin: 1.5rem 0; border: none; border-top: 1px solid #e0e0e0; }
    .modal-divider-light { margin: 0.75rem 0; border: none; border-top: 1px solid #f4f4f4; }
    
    /* --- Styles for the updated table cell --- */
    .rules-cell-container {
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
		padding: 8px 0;
    }
    .rule-item {
        padding: 4px;
        border-radius: 4px;
        transition: background-color 0.2s;
    }
    .rule-item.clickable {
        cursor: pointer;
    }
    .rule-item.clickable:hover {
        background-color: var(--cds-hover-ui, #e5e5e5);
    }
    .rule-item-header {
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    .rule-details {
        margin-top: 0.5rem;
        padding-left: 24px;
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
    }
    .message-card {
        background-color: var(--cds-background, #f4f4f4);
        border-left: 3px solid var(--cds-support-03, #ff832b);
        padding: 0.5rem 1rem;
        border-radius: 4px;
    }
    .message-card p {
        margin: 0.2rem 0;
        font-size: 13px;
    }

	/* --- Final CSS fix for the icon button size --- */
	:global(button.cds--btn--icon-only) {
		min-width: 2.5rem !important;
		width: 2.5rem !important;
		flex-grow: 0 !important;
	}

    .insights-virtual-table-container :global(.cell) {
        white-space: normal;
        overflow: hidden;
        height: auto;
    }

	/* Add some padding to the simple cells to vertically align content */
	.cell-padding {
		padding-top: 8px;
	}
</style>