//! Markdown report writers for offline dedup outputs and paper-style summaries.

use std::fs;
use std::path::Path;

use serde_json::Value;

use crate::error::AnalysisError;

use super::aggregate::DuplicateScaleRow;
use super::json::SeedDedupReport;

const DUPLICATE_SCALE_HEADER: &str = "| Category | Duplicate NFTs | NFT share | Duplicate contracts | Contract share |\n| --- | ---: | ---: | ---: | ---: |\n";

fn write_text(path: &Path, body: &str) -> Result<(), AnalysisError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, body)?;
    Ok(())
}

fn f64_cell(v: &Value) -> String {
    match v {
        Value::Number(n) => n
            .as_f64()
            .map(|x| {
                if x == 0.0 {
                    "0.000000".into()
                } else {
                    format!("{x:.6}")
                }
            })
            .unwrap_or_else(|| n.to_string()),
        Value::Null => "null".into(),
        other => other.to_string(),
    }
}

fn u64_cell(v: &Value) -> String {
    match v {
        Value::Number(n) => n
            .as_u64()
            .map(|x| x.to_string())
            .or_else(|| n.as_i64().map(|x| x.to_string()))
            .unwrap_or_else(|| n.to_string()),
        Value::Null => "0".into(),
        other => other.to_string(),
    }
}

fn percent_value(ratio: f64) -> String {
    let percent = ratio * 100.0;
    if percent == 0.0 || percent.abs() >= 0.01 {
        format!("{percent:.2}%")
    } else {
        format!("{percent:.6}%")
    }
}

fn pct_cell(ratio: &Value, numer: &Value, denom: &Value) -> String {
    match ratio.as_f64() {
        Some(r) if denom.as_u64().unwrap_or(0) > 0 || denom.as_f64().unwrap_or(0.0) > 0.0 => {
            format!(
                "{} ({}/{})",
                percent_value(r),
                u64_cell(numer),
                u64_cell(denom)
            )
        }
        Some(r) => percent_value(r),
        None => "null".into(),
    }
}

fn scale_table(rows: &[DuplicateScaleRow]) -> String {
    let mut out = String::from(DUPLICATE_SCALE_HEADER);
    for row in rows {
        let nft_ratio = row
            .duplicate_nft_ratio
            .map(|v| {
                format!(
                    "{} ({}/{})",
                    percent_value(v),
                    row.duplicate_nft_ratio_numerator,
                    row.duplicate_nft_ratio_denominator
                )
            })
            .unwrap_or_else(|| "null".into());
        let contract_ratio = row
            .duplicate_contract_ratio
            .map(|v| {
                format!(
                    "{} ({}/{})",
                    percent_value(v),
                    row.duplicate_contract_ratio_numerator,
                    row.duplicate_contract_ratio_denominator
                )
            })
            .unwrap_or_else(|| "null".into());
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            row.category,
            row.duplicate_nft_count,
            nft_ratio,
            row.duplicate_contract_count,
            contract_ratio
        ));
    }
    out
}

fn behavior_label(key: &str) -> String {
    match key {
        "wash_trading" => "Wash Trading".into(),
        "pump_and_exit" => "Pump-and-Exit".into(),
        "sybil_distribution" => "Sybil Distribution".into(),
        "fraud_revenue" => "Fraud Revenue".into(),
        "poisoning" => "Poisoning".into(),
        "layered_transfer" => "Layered Transfer".into(),
        "inventory_concentration" => "Inventory Concentration".into(),
        "total" => "total".into(),
        other => other.to_owned(),
    }
}

fn behavior_instance_ratio_cell(key: &str, row: &Value) -> String {
    match row.get("instance_ratio").and_then(Value::as_f64) {
        Some(ratio) => percent_value(ratio),
        None if key == "total"
            && row
                .get("instance_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                > 0 =>
        {
            "100.00%".into()
        }
        None if key == "total" => "n/a (0/0)".into(),
        None => "null".into(),
    }
}

fn wash_cycle_ratio_cell(row: &Value) -> String {
    if row["cycle_ratio_denominator"].as_u64() == Some(0) {
        "n/a (0/0)".into()
    } else {
        pct_cell(
            &row["cycle_ratio"],
            &row["cycle_ratio_numerator"],
            &row["cycle_ratio_denominator"],
        )
    }
}

pub fn write_seed_report_md(path: &Path, report: &SeedDedupReport) -> Result<(), AnalysisError> {
    write_text(path, &seed_dedup_md_body(report))
}

fn seed_dedup_md_body(report: &SeedDedupReport) -> String {
    let mut body = format!(
        "# Seed {} / {}\n\n- hit edges: {}\n- candidate contracts: {}\n\n",
        report.seed.chain,
        report.seed.address,
        report.hit_edge_count,
        report.candidate_contract_count
    );
    body.push_str("## Intra-chain\n\n");
    body.push_str(&scale_table(&report.duplicate_scale.intra_chain));
    body.push_str("\n## Cross-chain summary\n\n");
    body.push_str(&scale_table(&report.duplicate_scale.cross_chain_summary));
    for block in &report.duplicate_scale.chain_matrix {
        body.push_str(&format!(
            "\n## Chain matrix → {}\n\n",
            block.secondary_chain
        ));
        body.push_str(&scale_table(&block.rows));
    }
    if !report.relations.is_empty() {
        body.push_str(
            "\n## Candidates\n\n| chain | address | dimensions | nfts |\n|---|---|---|---:|\n",
        );
        for rel in &report.relations {
            body.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                rel.candidate_chain,
                rel.candidate_address,
                rel.dimensions.join(","),
                rel.nft_count
            ));
        }
    }
    body
}

pub fn write_seed_full_report_md(
    path: &Path,
    report: &super::run::SeedFullReport,
) -> Result<(), AnalysisError> {
    let mut body = seed_dedup_md_body(&report.dedup);
    body.push_str(&format!(
        "\n## Analysis\n\n- scopes_complete: {}\n- analysis_complete: {}\n",
        report.scopes_complete, report.analysis_complete
    ));
    if let Some(a) = &report.analysis {
        body.push_str(&format!(
            "- suspected_duplicate_contract_count: {}\n- legit_duplicate_contract_count: {}\n- infringing_nft_count: {}\n- honest_paid_exposure_usd: {}\n- operator_output_usd: {}\n",
            a.suspected_duplicate_contract_count,
            a.legit_duplicate_contract_count,
            a.infringing_nft_count,
            a.economics_usd.honest_loss_usd,
            a.economics_usd.operator_output_usd,
        ));
    }
    write_text(path, &body)
}

pub fn write_scope_md(
    path: &Path,
    scope: &str,
    reports: &[&SeedDedupReport],
    rows_of: impl Fn(&SeedDedupReport) -> &Vec<DuplicateScaleRow>,
) -> Result<(), AnalysisError> {
    let mut body = format!("# {scope}\n\n");
    for report in reports {
        body.push_str(&format!(
            "## {} / {}\n\n",
            report.seed.chain, report.seed.address
        ));
        body.push_str(&scale_table(rows_of(report)));
        body.push('\n');
    }
    write_text(path, &body)
}

pub fn write_matrix_md(path: &Path, reports: &[&SeedDedupReport]) -> Result<(), AnalysisError> {
    let mut body = String::from("# chain_matrix\n\n");
    for report in reports {
        for block in &report.duplicate_scale.chain_matrix {
            body.push_str(&format!(
                "## {} / {} → {}\n\n",
                report.seed.chain, report.seed.address, block.secondary_chain
            ));
            body.push_str(&scale_table(&block.rows));
            body.push('\n');
        }
    }
    write_text(path, &body)
}

pub fn write_summary_md(path: &Path, summary: &Value) -> Result<(), AnalysisError> {
    // Keep a thin alias for offline-dedup-only summary; full paper tables live in all_chains.
    write_all_chains_md(path, summary, &[])
}

/// Paper-style markdown for any of the four scopes (intra / matrix / cross / all_chains).
pub fn write_all_chains_md(
    path: &Path,
    summary: &Value,
    scale: &[DuplicateScaleRow],
) -> Result<(), AnalysisError> {
    let scope = summary
        .get("scope")
        .and_then(|v| v.as_str())
        .unwrap_or("all_chains");
    let mut body = format!("# NFT evidence summary (scope = {scope})\n\n");

    // Header counts. API/data-quality problems are documented separately and
    // never remove otherwise available seed results from this report.
    body.push_str(&format!(
        "- selected seeds: {}\n- included seed reports: {}\n- excluded seeds: {}\n- seed_with_duplicate: {} / {} ({})\n\n",
        summary["selected_seed_count"],
        summary["included_seed_report_count"],
        summary["excluded_seed_count"],
        summary["seed_with_duplicate_count"],
        summary["seed_duplicate_ratio_denominator"],
        f64_cell(&summary["seed_duplicate_ratio"]),
    ));

    body.push_str("## Duplicate scale\n\n");
    if scale.is_empty() {
        // Prefer embedded duplicate_scale from JSON when caller passes empty slice.
        if let Some(rows) = summary.get("duplicate_scale").and_then(|v| v.as_array()) {
            body.push_str(DUPLICATE_SCALE_HEADER);
            for row in rows {
                if row.get("enabled").and_then(Value::as_bool) == Some(false) {
                    body.push_str(&format!(
                        "| {} | n/a (disabled) | n/a | n/a (disabled) | n/a |\n",
                        row["category"].as_str().unwrap_or("?")
                    ));
                    continue;
                }
                let nft_n = u64_cell(&row["duplicate_nft_count"]);
                let c_n = u64_cell(&row["duplicate_contract_count"]);
                let nft_ratio = match row["duplicate_nft_ratio"].as_f64() {
                    Some(r) => format!(
                        "{} ({}/{})",
                        percent_value(r),
                        u64_cell(&row["duplicate_nft_ratio_numerator"]),
                        u64_cell(&row["duplicate_nft_ratio_denominator"])
                    ),
                    None => "null".into(),
                };
                let c_ratio = match row["duplicate_contract_ratio"].as_f64() {
                    Some(r) => format!(
                        "{} ({}/{})",
                        percent_value(r),
                        u64_cell(&row["duplicate_contract_ratio_numerator"]),
                        u64_cell(&row["duplicate_contract_ratio_denominator"])
                    ),
                    None => "null".into(),
                };
                body.push_str(&format!(
                    "| {} | {} | {} | {} | {} |\n",
                    row["category"].as_str().unwrap_or("?"),
                    nft_n,
                    nft_ratio,
                    c_n,
                    c_ratio
                ));
            }
        } else {
            body.push_str("_No duplicate-scale data_\n");
        }
    } else {
        body.push_str(&scale_table(scale));
    }

    // Matrix: additional per-secondary-chain scale tables.
    if let Some(blocks) = summary.get("matrix_blocks").and_then(|v| v.as_array()) {
        for block in blocks {
            let primary = block
                .get("primary_chain")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            let sec = block
                .get("secondary_chain")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            body.push_str(&format!("\n### Matrix {primary} → {sec}\n\n"));
            if let Some(rows) = block.get("rows").and_then(|v| v.as_array()) {
                body.push_str(DUPLICATE_SCALE_HEADER);
                for row in rows {
                    let nft_ratio = match row["duplicate_nft_ratio"].as_f64() {
                        Some(r) => format!(
                            "{} ({}/{})",
                            percent_value(r),
                            u64_cell(&row["duplicate_nft_ratio_numerator"]),
                            u64_cell(&row["duplicate_nft_ratio_denominator"])
                        ),
                        None => "null".into(),
                    };
                    let c_ratio = match row["duplicate_contract_ratio"].as_f64() {
                        Some(r) => format!(
                            "{} ({}/{})",
                            percent_value(r),
                            u64_cell(&row["duplicate_contract_ratio_numerator"]),
                            u64_cell(&row["duplicate_contract_ratio_denominator"])
                        ),
                        None => "null".into(),
                    };
                    body.push_str(&format!(
                        "| {} | {} | {} | {} | {} |\n",
                        row["category"].as_str().unwrap_or("?"),
                        u64_cell(&row["duplicate_nft_count"]),
                        nft_ratio,
                        u64_cell(&row["duplicate_contract_count"]),
                        c_ratio
                    ));
                }
            }
            if let Some(direction) = block.get("summary") {
                let econ = &direction["economics"];
                body.push_str("\n| Candidate contracts | Suspected contracts | Suspected infringing NFTs | Behavior contracts | Operator output USD | Buyer paid exposure USD | Gas USD |\n| ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
                body.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {} | {} |\n",
                    u64_cell(&direction["candidate_contract_count"]),
                    u64_cell(&direction["suspected_duplicate_contract_count"]),
                    u64_cell(&direction["infringing_nft_count"]),
                    u64_cell(&direction["behavior_contract_count"]),
                    f64_cell(&econ["operator_output_usd"]),
                    f64_cell(&econ["honest_paid_exposure_usd"]),
                    f64_cell(&econ["total_gas_usd"]),
                ));
            }
        }
    }

    if summary["analysis_available"].as_bool() == Some(false) {
        body.push_str(
            "\n## Evidence analysis\n\n_This run-dedup artifact contains duplicate scale and candidate counts only. Legitimacy, behavior, address, and economic analyses were not run; missing results are not represented as zero._\n",
        );
        return write_text(path, &body);
    }

    let addr = &summary["address_classification"];
    body.push_str("\n## Address classification\n\n");
    body.push_str("| Category | Malicious addresses | Repeat-infringement malicious addresses | Honest addresses | Total addresses |\n| --- | ---: | ---: | ---: | ---: |\n");
    body.push_str(&format!(
        "| all | {} | {} | {} | {} |\n",
        u64_cell(&addr["malicious_address_count"]),
        u64_cell(&addr["repeat_infringing_malicious_address_count"]),
        u64_cell(&addr["honest_address_count"]),
        u64_cell(&addr["total_address_count"]),
    ));
    body.push_str(&format!(
        "\n> Address classes are disjoint. The {} addresses with both malicious and honest roles are counted as malicious.\n",
        u64_cell(&addr["overlapping_role_address_count"]),
    ));

    let econ = &summary["economics"];
    body.push_str("\n## Attacker costs\n\n");
    body.push_str(
        "| cost | Setup Gas (USD) | Lure Gas (USD) | Exit Gas (USD) | Total Gas (USD) | Gas cost concentration |\n| --- | ---: | ---: | ---: | ---: | ---: |\n",
    );
    let conc = match econ["top_contract_gas_contribution_ratio"].as_f64() {
        Some(r) => format!(
            "{} ({}/{})",
            percent_value(r),
            f64_cell(&econ["top_contract_gas_contribution_numerator_usd"]),
            f64_cell(&econ["top_contract_gas_contribution_denominator_usd"])
        ),
        None => "null".into(),
    };
    body.push_str(&format!(
        "| gas | {} | {} | {} | {} | {} |\n",
        f64_cell(&econ["setup_gas_usd"]),
        f64_cell(&econ["lure_gas_usd"]),
        f64_cell(&econ["exit_gas_usd"]),
        f64_cell(&econ["total_gas_usd"]),
        conc,
    ));
    body.push_str(
        "\n> Amounts use spot USD prices obtained during execution. Unpriced payments are not counted as zero USD.\n",
    );
    if econ["evidence_coverage_complete"].as_bool() == Some(false) {
        body.push_str(
            "\n> On-chain evidence coverage is incomplete for this scope. Amounts describe observed evidence, not the full population.\n",
        );
    }
    if econ["observed_usd_pricing_complete"].as_bool() == Some(false) {
        body.push_str(
            "\n> Some observed sales, flows, mint payments, or gas costs lack reliable USD prices. Amounts describe the priced subset only.\n",
        );
    }
    if econ["operator_output_attribution_complete"].as_bool() == Some(false) {
        body.push_str(
            "\n> Some sale, royalty, or mint recipients could not be attributed to operators. Operator output is incomplete.\n",
        );
    }

    body.push_str("\n## Sales and operator output\n\n");
    body.push_str(
        "| Gross sales volume USD | Marketplace fees USD | Creator royalties USD | Operator royalties USD | Operator output USD |\n| ---: | ---: | ---: | ---: | ---: |\n",
    );
    body.push_str(&format!(
        "| {} | {} | {} | {} | {} |\n",
        f64_cell(&econ["gross_sales_volume_usd"]),
        f64_cell(&econ["marketplace_fee_usd"]),
        f64_cell(&econ["royalty_fee_usd"]),
        f64_cell(&econ["operator_royalty_usd"]),
        f64_cell(&econ["operator_output_usd"]),
    ));

    body.push_str("\n## Output/input ratio\n\n");
    body.push_str(
        "| scope | Comparable output USD | Comparable input USD (gas×spot) | Output/input | >=1 count share | <1 count share |\n| --- | ---: | ---: | ---: | ---: | ---: |\n",
    );
    let ratio_s = match econ["output_input_ratio"].as_f64() {
        Some(r) => format!("{r:.5}x"),
        None => "null".into(),
    };
    let ge1 = match econ["output_input_ratio_ge1_share"].as_f64() {
        Some(r) => format!(
            "{} ({}/{})",
            percent_value(r),
            u64_cell(&econ["output_input_ratio_ge1_count"]),
            u64_cell(&econ["output_input_ratio_count"])
        ),
        None => "null".into(),
    };
    let lt1 = match econ["output_input_ratio_lt1_share"].as_f64() {
        Some(r) => format!(
            "{} ({}/{})",
            percent_value(r),
            u64_cell(&econ["output_input_ratio_lt1_count"]),
            u64_cell(&econ["output_input_ratio_count"])
        ),
        None => "null".into(),
    };
    body.push_str(&format!(
        "| observed | {} | {} | {} | {} | {} |\n",
        f64_cell(
            econ.get("ratio_eligible_operator_output_usd")
                .unwrap_or(&econ["ratio_operator_output_usd"])
        ),
        f64_cell(
            econ.get("ratio_eligible_attacker_input_usd")
                .unwrap_or(&econ["attacker_input_usd"])
        ),
        ratio_s,
        ge1,
        lt1,
    ));
    let complete_ratio = match econ["complete_evidence_output_input_ratio"].as_f64() {
        Some(ratio) => format!("{ratio:.5}x"),
        None => "null".into(),
    };
    let complete_ge1 = match econ["complete_evidence_output_input_ratio_ge1_share"].as_f64() {
        Some(ratio) => format!(
            "{} ({}/{})",
            percent_value(ratio),
            u64_cell(&econ["complete_evidence_output_input_ratio_ge1_count"]),
            u64_cell(&econ["complete_evidence_output_input_ratio_count"])
        ),
        None => "null".into(),
    };
    let complete_lt1 = match econ["complete_evidence_output_input_ratio_lt1_share"].as_f64() {
        Some(ratio) => format!(
            "{} ({}/{})",
            percent_value(ratio),
            u64_cell(&econ["complete_evidence_output_input_ratio_lt1_count"]),
            u64_cell(&econ["complete_evidence_output_input_ratio_count"])
        ),
        None => "null".into(),
    };
    body.push_str(&format!(
        "| complete evidence | {} | {} | {} | {} | {} |\n",
        f64_cell(&econ["complete_evidence_ratio_operator_output_usd"]),
        f64_cell(&econ["complete_evidence_ratio_attacker_input_usd"]),
        complete_ratio,
        complete_ge1,
        complete_lt1,
    ));
    body.push_str(&format!(
        "\n> Ratios and >=1/<1 shares use the same {} comparable contracts. Candidate coverage complete: {}; evidence complete: {}; observed only: {}. All observed operator output: {} USD.\n",
        u64_cell(econ.get("ratio_eligible_contract_count").unwrap_or(&econ["output_input_ratio_count"])),
        econ["ratio_candidate_coverage_complete"],
        econ["ratio_evidence_complete"],
        econ["ratio_is_observed_only"],
        f64_cell(econ.get("all_observed_operator_output_usd").unwrap_or(&econ["operator_output_usd"])),
    ));
    body.push_str(&format!(
        "\n- candidate/operator funding_usd: {}\n- operator_internal_backflow_usd: {}\n- candidate/operator withdrawal_usd: {}\n",
        f64_cell(&econ["funding_usd"]),
        f64_cell(&econ["revenue_backflow_usd"]),
        f64_cell(&econ["withdrawal_usd"])
    ));

    let hit_contract_nfts = u64_cell(&econ["hit_contract_nft_count"]);
    let stuck = u64_cell(&econ["stuck_nft_count"]);
    let stuck_ratio = match econ["stuck_nft_ratio"].as_f64() {
        Some(r) => format!("{} ({stuck}/{hit_contract_nfts})", percent_value(r)),
        None => format!("n/a ({stuck}/{hit_contract_nfts})"),
    };
    body.push_str("\n## Honest buyer paid exposure\n\n");
    body.push_str(
        "| Stuck NFTs | Stuck NFT share | Secondary-sale paid exposure USD | Paid-mint exposure USD | Total paid exposure USD |\n| ---: | ---: | ---: | ---: | ---: |\n",
    );
    body.push_str(&format!(
        "| {stuck} | {stuck_ratio} | {} | {} | {} |\n",
        f64_cell(&econ["secondary_sale_paid_exposure_usd"]),
        f64_cell(&econ["paid_mint_exposure_usd"]),
        f64_cell(&econ["honest_paid_exposure_usd"]),
    ));

    body.push_str("\n## Malicious behavior summary\n\n");
    body.push_str(&format!(
        "- Contracts with observed malicious behavior: {}\n",
        u64_cell(
            summary
                .get("behavior_contract_count")
                .unwrap_or(&Value::Null)
        )
    ));
    body.push_str(&format!(
        "- Contracts with complete behavior evidence: {}\n",
        u64_cell(
            summary
                .get("behavior_analyzable_contract_count")
                .unwrap_or(&Value::Null)
        )
    ));
    body.push_str(
        "| Behavior | Observed contracts | Complete-evidence prevalence | Observed instances | Instance share | Addresses | NFTs | Linked buyers | Linked paid exposure USD |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
    let order = [
        "wash_trading",
        "pump_and_exit",
        "sybil_distribution",
        "fraud_revenue",
        "poisoning",
        "layered_transfer",
        "inventory_concentration",
        "total",
    ];
    if let Some(behaviors) = summary.get("behaviors").and_then(|v| v.as_object()) {
        for key in order {
            let Some(row) = behaviors.get(key) else {
                continue;
            };
            let contracts = u64_cell(&row["contract_count"]);
            let complete_contracts = u64_cell(&row["complete_evidence_contract_count"]);
            let analyzable = u64_cell(&row["behavior_analyzable_contract_count"]);
            let coverage = match row.get("contract_coverage_ratio").and_then(|v| v.as_f64()) {
                Some(r) => format!("{} ({complete_contracts}/{analyzable})", percent_value(r)),
                None => format!("n/a ({complete_contracts}/{analyzable})"),
            };
            let instances = u64_cell(&row["instance_count"]);
            let inst_ratio = behavior_instance_ratio_cell(key, row);
            body.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                behavior_label(key),
                contracts,
                coverage,
                instances,
                inst_ratio,
                u64_cell(&row["address_count"]),
                u64_cell(&row["nft_count"]),
                u64_cell(&row["linked_buyer_count"]),
                f64_cell(&row["linked_paid_exposure_usd"]),
            ));
        }
    }
    body.push_str(
        "\n> Behavior prevalence uses only contracts with complete required evidence. Non-detection under incomplete evidence is not treated as 0%.\n",
    );

    body.push_str("\n## Wash cycle sizes\n\n");
    body.push_str("| Nodes | Cycles | Cycle share |\n| --- | ---: | ---: |\n");
    if let Some(rows) = summary
        .get("wash_cycle_size_distribution")
        .and_then(|v| v.as_array())
    {
        for row in rows {
            let ratio = wash_cycle_ratio_cell(row);
            body.push_str(&format!(
                "| {} | {} | {} |\n",
                row["node_count_bucket"].as_str().unwrap_or("?"),
                u64_cell(&row["cycle_count"]),
                ratio
            ));
        }
    } else {
        body.push_str("| — | 0 | null |\n");
    }

    let dq = &summary["data_quality"];
    let gas = &dq["evidence"]["gas"];
    let pricing = &dq["pricing"];
    let dimensions = &dq["dedup_dimensions"];
    body.push_str("\n## Data quality\n\n");
    body.push_str(&format!(
        "- Representative candidate NFTs: {}\n- All NFTs in hit contracts: {}(complete: {})\n- Candidate contracts: {}\n- Suspected duplicate contracts: {}\n- Officially related duplicate contracts: {}\n- Suspected infringing NFTs: {}\n- Legitimate-relation verification Complete/Incomplete: {} / {}\n- Gas evidence Complete/Empty/Failed/Truncated/NotRequested: {} / {} / {} / {} / {}\n- Sale pricing Priced/Unpriced/Amountless/AssumedPeg/Total: {} / {} / {} / {} / {}\n- Operator net sale proceeds Priced/Unpriced/Unknown/Total: {} / {} / {} / {}\n- Royalty recipients Unknown: {}\n- Operator paid-mint receipts Priced/Unpriced/Operator/AllPaid/UnknownReceiver: {} / {} / {} / {} / {}\n- Honest buyer paid-mint exposure pricing Priced/Unpriced/Total: {} / {} / {}\n- Gas cost pricing Priced/Unpriced/Total: {} / {} / {}\n- Evidence coverage complete: {}\n- Observed USD pricing complete: {}\n- Operator attribution complete: {}\n- Operator output complete: {}\n- USD valuation complete: {}\n- Comparable output/input sample complete: {}\n- Dedup dimensions token_uri/image_uri/metadata/name: {} / {} / {} / {}\n",
        u64_cell(dq.get("representative_candidate_nft_count").unwrap_or(&summary["representative_candidate_nft_count"])),
        u64_cell(&dq["hit_contract_nft_count"]),
        dq["hit_contract_nft_count_complete"],
        u64_cell(dq.get("candidate_contract_count").unwrap_or(&summary["candidate_contract_count"])),
        u64_cell(dq.get("suspected_duplicate_contract_count").unwrap_or(&summary["suspected_duplicate_contract_count"])),
        u64_cell(dq.get("legit_duplicate_contract_count").unwrap_or(&summary["legit_duplicate_contract_count"])),
        u64_cell(dq.get("infringing_nft_count").unwrap_or(&summary["infringing_nft_count"])),
        u64_cell(&dq["legit_relation_verification_complete"]),
        u64_cell(&dq["legit_relation_verification_incomplete"]),
        u64_cell(&gas["complete"]),
        u64_cell(&gas["empty"]),
        u64_cell(&gas["failed"]),
        u64_cell(&gas["truncated"]),
        u64_cell(&gas["not_requested"]),
        u64_cell(&pricing["priced_sale_count"]),
        u64_cell(&pricing["unpriced_sale_count"]),
        u64_cell(&pricing["amountless_sale_count"]),
        u64_cell(&pricing["assumed_stablecoin_peg_sale_count"]),
        u64_cell(&pricing["sale_count"]),
        u64_cell(&pricing["priced_operator_sale_proceeds_count"]),
        u64_cell(&pricing["unpriced_operator_sale_proceeds_count"]),
        u64_cell(&pricing["unknown_operator_sale_proceeds_count"]),
        u64_cell(&pricing["operator_sale_count"]),
        u64_cell(&pricing["unknown_royalty_recipient_count"]),
        u64_cell(&pricing["priced_operator_paid_mint_payment_count"]),
        u64_cell(&pricing["unpriced_operator_paid_mint_payment_count"]),
        u64_cell(&pricing["operator_paid_mint_payment_count"]),
        u64_cell(&pricing["paid_mint_payment_count"]),
        u64_cell(&pricing["unknown_paid_mint_receiver_count"]),
        u64_cell(&pricing["priced_honest_paid_mint_exposure_count"]),
        u64_cell(&pricing["unpriced_honest_paid_mint_exposure_count"]),
        u64_cell(&pricing["honest_paid_mint_exposure_count"]),
        u64_cell(&pricing["priced_gas_cost_contract_count"]),
        u64_cell(&pricing["unpriced_gas_cost_contract_count"]),
        u64_cell(&pricing["gas_cost_contract_count"]),
        pricing["evidence_coverage_complete"],
        pricing["observed_usd_pricing_complete"],
        pricing["operator_output_attribution_complete"],
        pricing["operator_output_complete"],
        pricing["usd_valuation_complete"],
        econ["ratio_sample_complete"],
        dimensions["token_uri_enabled"],
        dimensions["image_uri_enabled"],
        dimensions["metadata_enabled"],
        dimensions["name_enabled"],
    ));
    if let Some(reasons) = dq
        .get("truncation_reason_counts")
        .and_then(|value| value.as_object())
        .filter(|reasons| !reasons.is_empty())
    {
        body.push_str("- Truncation reasons (contracts):\n");
        for (reason, count) in reasons {
            body.push_str(&format!("  - {reason}: {}\n", u64_cell(count)));
        }
    }

    write_text(path, &body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn zero_total_behavior_instances_are_not_reported_as_one_hundred_percent() {
        let empty = json!({"instance_count": 0, "instance_ratio": null});
        let populated = json!({"instance_count": 2, "instance_ratio": null});
        assert_eq!(behavior_instance_ratio_cell("total", &empty), "n/a (0/0)");
        assert_eq!(behavior_instance_ratio_cell("total", &populated), "100.00%");
    }

    #[test]
    fn zero_wash_cycles_are_not_reported_as_null() {
        let row = json!({
            "cycle_ratio": null,
            "cycle_ratio_numerator": 0,
            "cycle_ratio_denominator": 0
        });
        assert_eq!(wash_cycle_ratio_cell(&row), "n/a (0/0)");
    }
}
