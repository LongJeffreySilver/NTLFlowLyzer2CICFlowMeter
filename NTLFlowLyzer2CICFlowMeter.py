
import argparse
import pandas as pd

# MAPPING: key = final name (first list), value = original name in CSV
MAPPING = {
    "src_ip":                       "src_ip",
    "dst_ip":                       "dst_ip",
    "src_port":                     "src_port",
    "dst_port":                     "dst_port",
    "protocol":                     "protocol",
    "timestamp":                    "timestamp",
    "flow_duration":                "duration",
    "flow_byts_s":                  "bytes_rate",
    "flow_pkts_s":                  "packets_rate",
    "fwd_pkts_s":                   "fwd_packets_rate",
    "bwd_pkts_s":                   "bwd_packets_rate",
    "tot_fwd_pkts":                 "fwd_packets_count",
    "tot_bwd_pkts":                 "bwd_packets_count",
    "totlen_fwd_pkts":              "fwd_total_payload_bytes",
    "totlen_bwd_pkts":              "bwd_total_payload_bytes",
    "fwd_pkt_len_max":              "fwd_payload_bytes_max",
    "fwd_pkt_len_min":              "fwd_payload_bytes_min",
    "fwd_pkt_len_mean":             "fwd_payload_bytes_mean",
    "fwd_pkt_len_std":              "fwd_payload_bytes_std",
    "bwd_pkt_len_max":              "bwd_payload_bytes_max",
    "bwd_pkt_len_min":              "bwd_payload_bytes_min",
    "bwd_pkt_len_mean":             "bwd_payload_bytes_mean",
    "bwd_pkt_len_std":              "bwd_payload_bytes_std",
    "pkt_len_max":                  "payload_bytes_max",
    "pkt_len_min":                  "payload_bytes_min",
    "pkt_size_avg":                 "payload_bytes_mean",
    "pkt_len_mean":                 "payload_bytes_mean",
    "pkt_len_std":                  "payload_bytes_std",
    "pkt_len_var":                  "payload_bytes_variance",
    "fwd_header_len":               "fwd_total_header_bytes",
    "bwd_header_len":               "bwd_total_header_bytes",
    "fwd_seg_size_min":             "fwd_segment_size_min",
    "fwd_act_data_pkts":            "avg_fwd_packets_per_bulk",
    "flow_iat_mean":                "packets_IAT_mean",
    "flow_iat_max":                 "packet_IAT_max",
    "flow_iat_min":                 "packet_IAT_min",
    "flow_iat_std":                 "packet_IAT_std",
    "fwd_iat_tot":                  "fwd_packets_IAT_total",
    "fwd_iat_max":                  "fwd_packets_IAT_max",
    "fwd_iat_min":                  "fwd_packets_IAT_min",
    "fwd_iat_mean":                 "fwd_packets_IAT_mean",
    "fwd_iat_std":                  "fwd_packets_IAT_std",
    "bwd_iat_tot":                  "bwd_packets_IAT_total",
    "bwd_iat_max":                  "bwd_packets_IAT_max",
    "bwd_iat_min":                  "bwd_packets_IAT_min",
    "bwd_iat_mean":                 "bwd_packets_IAT_mean",
    "bwd_iat_std":                  "bwd_packets_IAT_std",
    "fwd_psh_flags":                "fwd_psh_flag_counts",
    "bwd_psh_flags":                "bwd_psh_flag_counts",
    "fwd_urg_flags":                "fwd_urg_flag_counts",
    "bwd_urg_flags":                "bwd_urg_flag_counts",
    "fin_flag_cnt":                 "fin_flag_counts",
    "syn_flag_cnt":                 "syn_flag_counts",
    "rst_flag_cnt":                 "rst_flag_counts",
    "psh_flag_cnt":                 "psh_flag_counts",
    "ack_flag_cnt":                 "ack_flag_counts",
    "urg_flag_cnt":                 "urg_flag_counts",
    "ece_flag_cnt":                 "ece_flag_counts",
    "down_up_ratio":                "down_up_rate",
    "pkt_size_avg":                 "payload_bytes_mean",
    "init_fwd_win_byts":            "fwd_init_win_bytes",
    "init_bwd_win_byts":            "bwd_init_win_bytes",
    "active_max":                   "active_max",
    "active_min":                   "active_min",
    "active_mean":                  "active_mean",
    "active_std":                   "active_std",
    "idle_max":                     "idle_max",
    "idle_min":                     "idle_min",
    "idle_mean":                    "idle_mean",
    "idle_std":                     "idle_std",
    "fwd_byts_b_avg":               "avg_fwd_bytes_per_bulk",
    "fwd_pkts_b_avg":               "avg_fwd_packets_per_bulk",
    "bwd_byts_b_avg":               "avg_bwd_bytes_per_bulk",
    "bwd_pkts_b_avg":               "avg_bwd_packets_bulk_rate",
    "fwd_blk_rate_avg":             "avg_fwd_bulk_rate",
    "bwd_blk_rate_avg":             "avg_bwd_bulk_rate",
    "fwd_seg_size_avg":             "fwd_segment_size_mean",
    "bwd_seg_size_avg":             "bwd_segment_size_mean",
    "cwr_flag_count":               "cwr_flag_counts",
    "subflow_fwd_pkts":             "subflow_fwd_packets",
    "subflow_bwd_pkts":             "subflow_bwd_packets",
    "subflow_fwd_byts":             "subflow_fwd_bytes",
    "subflow_bwd_byts":             "subflow_bwd_bytes",
}

def build_rename_dict(mapping: dict) -> dict:
    rename_dict = {}
    seen_orig = set()

    for new_name, orig_name in mapping.items():
        if orig_name not in seen_orig:
            rename_dict[orig_name] = new_name
            seen_orig.add(orig_name)
    return rename_dict

def main(input_csv: str, output_csv: str):
    df = pd.read_csv(input_csv)

    rename_dict = build_rename_dict(MAPPING)

    df_renamed = df.rename(columns=rename_dict)

    if "pkt_size_avg" in df_renamed.columns:
        df_renamed["pkt_len_mean"] = df_renamed["pkt_size_avg"]
    else:
        df_renamed["pkt_len_mean"] = pd.NA

    desired_cols = list(MAPPING.keys())
    for col in desired_cols:
        if col not in df_renamed.columns:
            df_renamed[col] = pd.NA

    df_final = df_renamed[desired_cols]

    df_final.to_csv(output_csv, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Map CSV headers from second-list (NTLFlowLyzer) to first-list (CICFlowMeter) names')
    parser.add_argument('input_csv', help='Path to input CSV (with second-list headers)')
    parser.add_argument('output_csv', help='Path to output CSV (with first-list headers)')
    args = parser.parse_args()

    main(args.input_csv, args.output_csv)
