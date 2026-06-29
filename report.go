package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

// txTypeNames maps a type index to a human-readable label.
var txTypeNames = [numTxTypes]string{
	"0x00 LegacyTx",
	"0x01 AccessListTx (EIP-2930)",
	"0x02 DynamicFeeTx (EIP-1559)",
	"0x03 BlobTx (EIP-4844)",
	"0x04 SetCodeTx (EIP-7702)",
}

// TypeStat is one tx-type's count and percentage for JSON output.
type TypeStat struct {
	Type       string  `json:"type"`
	Name       string  `json:"name"`
	Count      int64   `json:"count"`
	Percentage float64 `json:"percentage"`
}

// Summary is the summary.json document.
type Summary struct {
	GeneratedAt      string     `json:"generated_at"`
	StartDate        string     `json:"start_date"`
	EndDate          string     `json:"end_date"`
	StartBlock       int64      `json:"start_block"`
	EndBlock         int64      `json:"end_block"`
	TotalTx          int64      `json:"total_tx"`
	ContractCreation int64      `json:"contract_creation_tx"`
	FailedBlocks     int64      `json:"failed_blocks"`
	TypeStats        []TypeStat `json:"type_stats"`
}

func pct(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

func buildSummary(m *Merged, cfg *Config, startBlock, endBlock, failedBlocks int64) Summary {
	s := Summary{
		GeneratedAt:      time.Now().UTC().Format(time.RFC3339),
		StartDate:        cfg.StartDate.Format("2006-01-02 15:04:05 MST"),
		EndDate:          cfg.EndDate.Format("2006-01-02 15:04:05 MST"),
		StartBlock:       startBlock,
		EndBlock:         endBlock,
		TotalTx:          m.TotalTx,
		ContractCreation: m.ContractCreation,
		FailedBlocks:     failedBlocks,
	}
	for i := 0; i < numTxTypes; i++ {
		s.TypeStats = append(s.TypeStats, TypeStat{
			Type:       fmt.Sprintf("0x%02x", i),
			Name:       txTypeNames[i],
			Count:      m.TypeCounts[i],
			Percentage: pct(m.TypeCounts[i], m.TotalTx),
		})
	}
	return s
}

// writeJSONFile writes v as pretty JSON.
func writeJSONFile(path string, v interface{}) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0644)
}

// writeCSV writes an address ranking CSV: address,count,percentage.
func writeCSV(path string, rows []AddrCount) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write([]string{"address", "count", "percentage"}); err != nil {
		return err
	}
	for _, r := range rows {
		rec := []string{
			r.Address,
			strconv.FormatInt(r.Count, 10),
			strconv.FormatFloat(r.Percentage, 'f', 6, 64),
		}
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// writeReportMD writes the merged human-readable Markdown report.
func writeReportMD(path string, s Summary, contracts, eoa []AddrCount) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "# BSC Transaction Statistics\n\n")
	fmt.Fprintf(f, "- Generated: %s\n", s.GeneratedAt)
	fmt.Fprintf(f, "- Range: %s .. %s\n", s.StartDate, s.EndDate)
	fmt.Fprintf(f, "- Blocks: %d .. %d (%d blocks)\n", s.StartBlock, s.EndBlock, s.EndBlock-s.StartBlock+1)
	fmt.Fprintf(f, "- Total transactions: %d\n", s.TotalTx)
	fmt.Fprintf(f, "- Contract-creation tx (to == null): %d (%.4f%%)\n", s.ContractCreation, pct(s.ContractCreation, s.TotalTx))
	if s.FailedBlocks > 0 {
		fmt.Fprintf(f, "- ⚠️ Incomplete: %d block(s) failed all retries and are NOT counted above; rerun with -rescan_failed\n", s.FailedBlocks)
	}
	fmt.Fprintf(f, "\n")

	fmt.Fprintf(f, "## Transaction Types\n\n")
	fmt.Fprintf(f, "| Type | Name | Count | Percentage |\n")
	fmt.Fprintf(f, "|------|------|------:|-----------:|\n")
	for _, t := range s.TypeStats {
		fmt.Fprintf(f, "| %s | %s | %d | %.4f%% |\n", t.Type, t.Name, t.Count, t.Percentage)
	}
	fmt.Fprintf(f, "\n")

	writeTopTable(f, "Top 100 Contract To-Addresses", contracts)
	writeTopTable(f, "Top 100 EOA To-Addresses", eoa)
	return nil
}

func writeTopTable(f *os.File, title string, rows []AddrCount) {
	fmt.Fprintf(f, "## %s\n\n", title)
	fmt.Fprintf(f, "| Rank | Address | Count | Percentage |\n")
	fmt.Fprintf(f, "|-----:|---------|------:|-----------:|\n")
	for i, r := range rows {
		fmt.Fprintf(f, "| %d | %s | %d | %.6f%% |\n", i+1, r.Address, r.Count, r.Percentage)
	}
	fmt.Fprintf(f, "\n")
}

// writeOutputs produces all four output files.
func writeOutputs(cfg *Config, m *Merged, startBlock, endBlock, failedBlocks int64, contracts, eoa []AddrCount) error {
	s := buildSummary(m, cfg, startBlock, endBlock, failedBlocks)
	if err := writeJSONFile(outFile(cfg.OutDir, "summary.json"), s); err != nil {
		return fmt.Errorf("summary.json: %w", err)
	}
	if err := writeCSV(outFile(cfg.OutDir, "top100_contracts.csv"), contracts); err != nil {
		return fmt.Errorf("top100_contracts.csv: %w", err)
	}
	if err := writeCSV(outFile(cfg.OutDir, "top100_eoa.csv"), eoa); err != nil {
		return fmt.Errorf("top100_eoa.csv: %w", err)
	}
	if err := writeReportMD(outFile(cfg.OutDir, "report.md"), s, contracts, eoa); err != nil {
		return fmt.Errorf("report.md: %w", err)
	}
	return nil
}
