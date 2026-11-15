#!/usr/bin/env python3
"""Generate tests/fixtures/rik_synthetic_bundle.txt: SYNTHETIC data in the exact RIK file formats.

All companies, numbers and names are invented. Only the formats copy the real RIK open-data files:
';' separator, CRLF, header row, string fields double-quoted (company basic data unquoted),
dates dd.mm.yyyy, '.' decimals. Bundle layout = the real sample bundle ('@@@FILE <name>' sections).

Deliberately awkward cases:
  * quoted ';' and doubled quotes ("") inside fields (elements labels, auditor decision);
  * empty quoted strings "" and empty unquoted fields;
  * Estonian letters in names, labels and table names;
  * an exact duplicate report row (report 900011) in aruannete_yldandmed.csv;
  * two element rows with the same (report_id, tabel, elemendi_nimetus='') but different labels;
  * a UTF-8 BOM at the start of emtak_myygitulu.csv;
  * a 2021 report and companies outside the 'registrikood % 500 == 7' sample rule;
  * a company with reports but no row in the company basic-data file (deleted company).

Run: python tests/fixtures/make_synthetic_fixture.py   (deterministic output)
"""

from pathlib import Path

OUT = Path(__file__).resolve().parent / "rik_synthetic_bundle.txt"
CRLF = "\r\n"


def q(v):
    """Quote like RIK does for string fields."""
    return '"' + str(v).replace('"', '""') + '"'


GENERAL_HEADER = ('"report_id";"taidetud_aruanne_report_id";"registrikood";"õiguslik vorm";"staatus";'
                  '"aruandeaasta";"kas konsolideeritud?";"period_start";"period_end";"esitatud_kpv";'
                  '"kas auditeeritud?";"valitud aruanne kategooria";"minimaalne kategooria andmete alusel";'
                  '"auditi töövõtu liik";"audiitori otsuse tüüp";"modifikatsioon asjaolu rõhutamine";'
                  '"modifikatsioon muu asjaolu";"modifikatsioon tegevuse jatkuvus";"Audiitorettevõtja (AEV)"')
ELEMENTS_HEADER = '"report_id";"tabel";"elemendi_label";"elemendi_nimetus";"vaartus"'
EMTAK_HEADER = '"report_id";"emtak";"Jaotatud müügitulu";"põhitegevusala";"emtak_version"'
BASIC_HEADER = ("nimi;ariregistri_kood;ettevotja_oiguslik_vorm;ettevotja_oigusliku_vormi_alaliik;kmkr_nr;"
                "ettevotja_staatus;ettevotja_staatus_tekstina;ettevotja_esmakande_kpv;ettevotja_aadress;"
                "asukoht_ettevotja_aadressis;asukoha_ehak_kood;asukoha_ehak_tekstina;indeks_ettevotja_aadressis;"
                "ads_adr_id;ads_ads_oid;ads_normaliseeritud_taisaadress;teabesysteemi_link")

# 20 synthetic companies: (registry code, name, legal form short, legal form long, county text, emtak)
COMPANIES = [
    ("10000007", "Sünteetiline Õunaaed OÜ", "OÜ", "Osaühing", "Tartu linn, Tartu maakond", "01241"),
    ("10000507", "Näidis Ehitus AS", "AS", "Aktsiaselts", "Kesklinna linnaosa, Tallinn, Harju maakond", "41201"),
    ("10001007", "Testižürii Konsult OÜ", "OÜ", "Osaühing", "Pärnu linn, Pärnu maakond", "70221"),
    ("10001507", "Šokolaadi Proov OÜ", "OÜ", "Osaühing", "Narva linn, Ida-Viru maakond", "10821"),
    ("10002007", "Väike Kohvik OÜ", "OÜ", "Osaühing", "Haapsalu linn, Lääne maakond", "56101"),
    ("10002507", "Suur Logistika AS", "AS", "Aktsiaselts", "Rae vald, Harju maakond", "49411"),
    ("10003007", "Ühistu Näide TÜ", "TÜ", "Tulundusühistu", "Viljandi linn, Viljandi maakond", "01111"),
    ("10003507", "Kustutatud Firma OÜ", "OÜ", "Osaühing", "Tartu linn, Tartu maakond", "62011"),
    ("10004007", "Mõõdukas Tarkvara OÜ", "OÜ", "Osaühing", "Mustamäe linnaosa, Tallinn, Harju maakond", "62011"),
    ("10004507", "Jõe Kalandus OÜ", "OÜ", "Osaühing", "Võru linn, Võru maakond", "03111"),
    ("10000018", "Mitte Valimis OÜ", "OÜ", "Osaühing", "Tartu linn, Tartu maakond", "47111"),
    ("10000029", "Teine Väline AS", "AS", "Aktsiaselts", "Rakvere linn, Lääne-Viru maakond", "46901"),
    ("10000505", "Kolmas Väline OÜ", "OÜ", "Osaühing", "Kuressaare linn, Saare maakond", "55101"),
    ("10005007", "Ökoloogia Lahendused OÜ", "OÜ", "Osaühing", "Paide linn, Järva maakond", "38211"),
    ("10005507", "Tööstus & Partnerid AS", "AS", "Aktsiaselts", "Jõhvi linn, Ida-Viru maakond", "25621"),
    ("10006007", "Kaubandus Pluss OÜ", "OÜ", "Osaühing", "Pirita linnaosa, Tallinn, Harju maakond", "47191"),
    ("10006507", "Õpetus ja Koolitus MTÜ", "MTÜ", "Mittetulundusühing", "Tartu linn, Tartu maakond", "85599"),
    ("10007007", "Põllu Masinad OÜ", "OÜ", "Osaühing", "Jõgeva linn, Jõgeva maakond", "46611"),
    ("10007507", "Meri ja Sadam AS", "AS", "Aktsiaselts", "Paldiski linn, Harju maakond", "52221"),
    ("10008007", "Lõuna Energia OÜ", "OÜ", "Osaühing", "Valga linn, Valga maakond", "35111"),
]
DELETED = {"10003507"}  # has reports, missing from the company basic-data file


def build():
    general, elements, emtak, basic = [GENERAL_HEADER], {y: [ELEMENTS_HEADER] for y in range(2022, 2026)}, \
        [EMTAK_HEADER], [BASIC_HEADER]
    rid = 900000
    for i, (code, name, short, long_, county, nace) in enumerate(COMPANIES):
        years = [2021, 2022, 2023, 2024, 2025] if i < 3 else [2023, 2024, 2025]
        for y in years:
            rid += 1
            fte = [3.5, 9.99, 10, 49, 50, 249.5, 250, 1200][(i + y) % 8] if i != 4 else None
            revenue = 100000 * (i + 1) + 7000 * (y - 2020)
            audit_decision = "Märkuseta" if i % 3 else 'Märkusega; vt "Lisa 5"'
            general.append(";".join([
                str(rid), "" if i % 4 else str(rid - 1), code, q(short), q("Registrisse kantud"), str(y),
                q("Jah" if i == 5 else "Ei"), q(f"01.01.{y}"), q(f"31.12.{y}"), q(f"30.06.{y + 1}"),
                q("Jah" if i % 2 else "Ei"), q("Mikroettevõtja"), q("Väikeettevõtja"),
                q("Audit" if i % 2 else ""), q(audit_decision if i % 2 else ""), q(""), q(""), q(""),
                q("10384467" if i % 2 else ""),
            ]))
            if rid == 900011:  # exact duplicate row, as a re-delivered record would look
                general.append(general[-1])
            if y in elements:
                rows = [("Bilanss", "Varad kokku", "Assets", revenue * 2),
                        ("Bilanss", "Käibevarad", "CurrentAssets", revenue),
                        ("Kasumiaruanne", "Müügitulu", "Revenue", revenue),
                        ("Kasumiaruanne", "Aruandeaasta kasum (kahjum)", "TotalAnnualPeriodProfitLoss",
                         revenue * 0.05 - 1000 * i),
                        ("Lisa: Tööjõukulud", "Muud tulud; kulud (sünteetiline)", "", 12.5),
                        ("Lisa: Tööjõukulud", 'Silt ""jutumärkidega""', "", 0)]
                if fte is not None:
                    rows.append(("Lisa: Tööjõukulud", "Keskmine töötajate arv täistööajale taandatuna",
                                 "AverageNumberOfEmployeesInFullTimeEquivalentUnits", fte))
                for tabel, label, element, value in rows:
                    label_q = '"' + label + '"' if '""' in label else q(label)
                    elements[y].append(";".join([str(rid), q(tabel), label_q, q(element), q(f"{float(value):.1f}")]))
            emtak.append(";".join([str(rid), q(nace), q(str(revenue)), q("jah"), q("EMTAK 2008")]))
            if i == 1:  # secondary activity
                emtak.append(";".join([str(rid), q("43991"), q("0"), q("ei"), q("EMTAK 2008")]))
        if code not in DELETED:
            last = county.split(", ")[-1]
            basic.append(";".join([
                name, code, long_, "", f"EE10{code}" if i % 2 else "", "R", "Registrisse kantud",
                f"0{1 + i % 9}.0{1 + i % 9}.20{10 + i}", "", f"Näidistänav {i + 1}", f"0{100 + i}", county,
                f"{10000 + i}", str(2000000 + i), "", f"{last}, Näidistänav {i + 1}",
                f"https://ariregister.rik.ee/est/company/{code}",
            ]))

    files = [
        ("_README_SYNTHETIC.txt", ["SYNTHETIC TEST FIXTURE - invented companies and numbers, real RIK file formats.",
                                   "Generated by tests/fixtures/make_synthetic_fixture.py"]),
        ("aruannete_yldandmed.csv", general),
        *[(f"aruannete_elemendid_{y}.csv", elements[y]) for y in range(2022, 2026)],
        ("emtak_myygitulu.csv", emtak),
        ("ettevotja_lihtandmed.csv", basic),
    ]
    with open(OUT, "w", encoding="utf-8", newline="") as fh:
        for name, lines in files:
            fh.write(f"@@@FILE {name}{CRLF}")
            if name == "emtak_myygitulu.csv":
                fh.write("﻿")
            fh.write(CRLF.join(lines) + CRLF)
    return {name: len(lines) - (0 if name.startswith("_") else 1) for name, lines in files}


if __name__ == "__main__":
    for k, v in build().items():
        print(f"{k}: {v}")
