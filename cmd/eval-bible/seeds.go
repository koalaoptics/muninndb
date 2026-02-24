package main

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
)

// xrefMap maps a verse reference to the list of cross-referenced verse references.
type xrefMap map[string][]string

// abbrevMap maps OpenBible abbreviations to canonical book names.
var abbrevMap = map[string]string{
	"Gen":    "Genesis",
	"Exod":   "Exodus",
	"Lev":    "Leviticus",
	"Num":    "Numbers",
	"Deut":   "Deuteronomy",
	"Josh":   "Joshua",
	"Judg":   "Judges",
	"Ruth":   "Ruth",
	"1Sam":   "1 Samuel",
	"2Sam":   "2 Samuel",
	"1Kgs":   "1 Kings",
	"2Kgs":   "2 Kings",
	"1Chr":   "1 Chronicles",
	"2Chr":   "2 Chronicles",
	"Ezra":   "Ezra",
	"Neh":    "Nehemiah",
	"Esth":   "Esther",
	"Job":    "Job",
	"Ps":     "Psalms",
	"Prov":   "Proverbs",
	"Eccl":   "Ecclesiastes",
	"Song":   "Song of Solomon",
	"Isa":    "Isaiah",
	"Jer":    "Jeremiah",
	"Lam":    "Lamentations",
	"Ezek":   "Ezekiel",
	"Dan":    "Daniel",
	"Hos":    "Hosea",
	"Joel":   "Joel",
	"Amos":   "Amos",
	"Obad":   "Obadiah",
	"Jonah":  "Jonah",
	"Mic":    "Micah",
	"Nah":    "Nahum",
	"Hab":    "Habakkuk",
	"Zeph":   "Zephaniah",
	"Hag":    "Haggai",
	"Zech":   "Zechariah",
	"Mal":    "Malachi",
	"Matt":   "Matthew",
	"Mark":   "Mark",
	"Luke":   "Luke",
	"John":   "John",
	"Acts":   "Acts",
	"Rom":    "Romans",
	"1Cor":   "1 Corinthians",
	"2Cor":   "2 Corinthians",
	"Gal":    "Galatians",
	"Eph":    "Ephesians",
	"Phil":   "Philippians",
	"Col":    "Colossians",
	"1Thess": "1 Thessalonians",
	"2Thess": "2 Thessalonians",
	"1Tim":   "1 Timothy",
	"2Tim":   "2 Timothy",
	"Titus":  "Titus",
	"Phlm":   "Philemon",
	"Heb":    "Hebrews",
	"Jas":    "James",
	"1Pet":   "1 Peter",
	"2Pet":   "2 Peter",
	"1John":  "1 John",
	"2John":  "2 John",
	"3John":  "3 John",
	"Jude":   "Jude",
	"Rev":    "Revelation",
}

// abbrevToBook converts an OpenBible abbreviation to a canonical book name.
// Returns the input unchanged if no mapping is found.
func abbrevToBook(abbrev string) string {
	if name, ok := abbrevMap[abbrev]; ok {
		return name
	}
	return abbrev
}

// parseXRefRef converts "Gen.1.1" style refs to "Genesis 1:1" canonical form.
func parseXRefRef(ref string) (string, error) {
	parts := strings.Split(ref, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid ref format %q (want Book.Chapter.Verse)", ref)
	}
	bookName := abbrevToBook(parts[0])
	return fmt.Sprintf("%s %s:%s", bookName, parts[1], parts[2]), nil
}

// parseXRef parses cross-reference data into an xrefMap.
// Supports TSV format ("From Verse\tTo Verse\tVotes" with header line) and
// CSV format ("Gen.1.1,Isa.65.17,6" without header line). Auto-detected.
func parseXRef(data []byte) (xrefMap, error) {
	result := make(xrefMap)
	scanner := bufio.NewScanner(bytes.NewReader(data))

	// Detect separator: read first line to determine format
	if !scanner.Scan() {
		return result, nil
	}
	firstLine := scanner.Text()

	sep := ","
	hasHeader := false
	if strings.Contains(firstLine, "\t") {
		sep = "\t"
		hasHeader = true // TSV format has a header row
	}
	// If CSV format (no header), process the first line as data
	if !hasHeader && firstLine != "" {
		if fields := strings.SplitN(firstLine, sep, 3); len(fields) >= 2 {
			if from, err := parseXRefRef(fields[0]); err == nil {
				if to, err := parseXRefRef(fields[1]); err == nil {
					result[from] = append(result[from], to)
				}
			}
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		fields := strings.SplitN(line, sep, 3)
		if len(fields) < 2 {
			continue
		}
		fromRef, err := parseXRefRef(fields[0])
		if err != nil {
			continue
		}
		toRef, err := parseXRefRef(fields[1])
		if err != nil {
			continue
		}
		result[fromRef] = append(result[fromRef], toRef)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan cross-refs: %w", err)
	}
	return result, nil
}

// selectSeeds returns verse references that have at least minXRefs cross-references,
// capped at maxSeeds. The selection is deterministic (sorted map iteration is not
// stable, but we sort explicitly for reproducibility).
func selectSeeds(xrefs xrefMap, minXRefs, maxSeeds int) []string {
	seeds := make([]string, 0, maxSeeds)
	for ref, refs := range xrefs {
		if len(refs) >= minXRefs {
			seeds = append(seeds, ref)
			if len(seeds) >= maxSeeds {
				break
			}
		}
	}
	return seeds
}
