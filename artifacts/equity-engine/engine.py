"""
Equity Ranking Engine
Fetches real market data from Yahoo Finance, computes momentum/quality factors,
ranks stocks, and provides portfolio risk metrics.
"""

import os
import json
import time
import threading
import logging
from datetime import datetime, timedelta
from typing import Optional, Any

import numpy as np
import pandas as pd
import yfinance as yf
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import AgglomerativeClustering
import diskcache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = "/tmp/equity_cache"
CACHE_TTL = 8 * 3600  # 8 hours

cache = diskcache.Cache(CACHE_DIR)

# Status tracking
_status = {
    "status": "loading",
    "message": "Initializing...",
    "progress": 0,
    "total": 0,
    "loaded": 0,
    "cached_at": None,
}
_status_lock = threading.Lock()

# Universe of ~1000 large liquid US stocks
# S&P 500 + Russell 1000 subset filtered to common stocks
UNIVERSE_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "JPM", "LLY",
    "V", "UNH", "XOM", "MA", "COST", "JNJ", "PG", "HD", "WMT", "MRK",
    "ABBV", "CVX", "BAC", "KO", "NFLX", "PEP", "CRM", "TMO", "ORCL", "ACN",
    "MCD", "AMD", "PM", "LIN", "CSCO", "GE", "ABT", "NOW", "DIS", "WFC",
    "CAT", "TXN", "NEE", "INTU", "IBM", "CMCSA", "ISRG", "SPGI", "GS", "AMGN",
    "QCOM", "RTX", "BKNG", "VZ", "HON", "LOW", "AMAT", "AXP", "PFE", "SCHW",
    "T", "UNP", "SYK", "BX", "MS", "UBER", "C", "DE", "MU", "BA",
    "ADI", "GILD", "ADP", "BLK", "ETN", "REGN", "PLD", "CB", "CI", "VRTX",
    "LRCX", "TJX", "MDLZ", "SO", "MMC", "EOG", "FI", "SHW", "BMY", "ICE",
    "CL", "PANW", "EQIX", "WM", "CME", "PH", "ZTS", "AON", "CARR", "KLAC",
    "MCO", "DUK", "SLB", "COF", "MAR", "TDG", "APH", "GEV", "APO", "MPC",
    "PSX", "WELL", "TT", "CTAS", "GD", "HCA", "OXY", "PCAR", "SNPS", "ECL",
    "CDNS", "CMG", "NOC", "MSI", "NKE", "FTNT", "ORLY", "CEG", "AFL", "AIG",
    "PSA", "WDAY", "TMUS", "WMB", "DVN", "EW", "HES", "COP", "AZO", "FANG",
    "PCG", "KMB", "ALL", "STZ", "RCL", "CBRE", "ITW", "D", "PPG", "DOW",
    "LHX", "PAYX", "HSY", "EXC", "GWW", "CCI", "FICO", "PEG", "HAL", "BIIB",
    "FAST", "ACGL", "VRSK", "BDX", "KEYS", "HLT", "KHC", "RSG", "CTSH", "NEM",
    "A", "OKE", "XEL", "LUV", "DAL", "UAL", "AAL", "F", "GM", "SBUX",
    "EBAY", "PYPL", "SQ", "SNOW", "PLTR", "RBLX", "COIN", "HOOD", "DKNG", "APP",
    "CRWD", "DDOG", "NET", "ZS", "OKTA", "MNDY", "HCP", "VEEV", "ANSS", "IDXX",
    "WAT", "WST", "EPAM", "MPWR", "ENPH", "FSLR", "ROP", "CPRT", "ODFL", "CSGP",
    "EXAS", "TTWO", "EA", "TAKE", "NTES", "BIDU", "JD", "PDD", "BABA", "NIO",
    "XPEV", "LI", "RIVN", "LCID", "F", "GM", "STLA", "TM", "HMC", "RACE",
    "ABNB", "LYFT", "Z", "OPEN", "EXPE", "TRIP", "PCLN", "NCLH", "CCL", "RCL",
    "MGM", "WYNN", "LVS", "PENN", "DRAFTK", "CHDN", "BYD",
    "MO", "PM", "BTI", "IMBBY", "HRNNF",
    "CVS", "WBA", "RAD",
    "WDC", "STX", "NTAP", "HPQ", "HPE", "DELL", "SMCI", "PSTG",
    "ANET", "JNPR", "CSCO",
    "INTC", "ON", "QCOM", "MRVL", "SWKS", "QRVO", "MPWR", "WOLF", "STM",
    "ACM", "PWR", "MTZ", "WLDN", "STRL", "ROAD",
    "PNR", "IR", "TDY", "LDOS", "BAH", "SAIC", "CACI", "MANT",
    "IEX", "RHM", "STRL",
    "ROST", "BURL", "FIVE", "OLLI", "BIG", "DG", "DLTR", "KR", "SFM",
    "AMZN", "ETSY", "WISH", "W",
    "ZM", "DOCU", "WORK", "TWLO", "BAND", "RNG", "NICE", "EGHT",
    "HUBS", "SPRK", "BILL", "GTLB", "NTNX", "PTC", "MANH",
    "PAYC", "PCTY", "WK", "CDAY", "AZPN", "DSGN",
    "CLX", "CHD", "EL", "ULTA", "IPAR", "ELF", "COTY",
    "TAP", "BUD", "SAM", "FIZZ", "MNST", "CELH", "SMAR",
    "CAG", "SJM", "CPB", "MKC", "HRL", "LANC", "SAFM", "TSN", "HFC",
    "WH", "CHH", "H", "HLT", "MAR", "IHG", "WYN",
    "EXP", "MLM", "VMC", "USCR", "SMID", "SLVM", "IP", "AVY", "PKG",
    "BCC", "UFP", "LPX",
    "DLR", "AMT", "CCI", "SBAC", "IRM", "EPR", "ARE", "BXP", "EQR", "AVB",
    "ESS", "MAA", "UDR", "CPT", "NNN", "O", "VICI", "GLPI", "STAG", "TRNO",
    "FR", "EGP", "REXR",
    "AJG", "WTW", "MMC", "MKL", "RE", "RLI", "ERIE",
    "OC", "SWK", "MAS", "FBHS", "TREX", "AZEK",
    "LII", "TT", "CARR", "JELD", "AWI", "CSL",
    "ROK", "EMR", "A", "TRMB", "BRKS", "MKSI",
    "DXCM", "PODD", "TNDM", "ALGN", "HOLX", "BIO", "TECH", "XRAY",
    "QDEL", "NVCR", "AXNX", "SILK", "INSP", "NVST",
    "MCK", "CAH", "COR", "PDCO", "HSIC", "PRGO",
    "HUM", "MOH", "CNC", "ELV", "CVS", "ANTM",
    "PKI", "IQV", "ICLR", "MEDP", "SYNH",
    "STE", "MMSI", "HAYW",
    "RLMD", "GDRX", "AMWL", "ONEM", "HIMS",
    "RMD", "PHG", "IHI",
    "AMP", "PFG", "VOYA", "BEN", "IVZ", "AMG",
    "NTRS", "STT", "TROW", "FHN", "KEY", "CMA", "ZION", "FHB",
    "FITB", "HBAN", "RF", "CFG", "ALLY", "SYF",
    "DFS", "COF", "AXP", "V", "MA", "STNE", "GPN", "FIS", "FLT",
    "WEX", "EEFT", "GDOT",
    "MTB", "BOKF", "EWBC", "COLB", "VLY", "PACW", "WAL", "FULT",
    "WTFC", "IBOC", "UMBF",
    "GL", "LNC", "MET", "PRU", "EQH", "CRBG",
    "AEP", "FE", "ES", "AEE", "CMS", "LNT", "NI", "PNW", "EVRG", "ATO",
    "SWX", "SR", "OGS", "NWN",
    "AWK", "WTR", "YORW", "MSEX", "GWRS",
    "OGE", "IDACORP", "MGEE",
    "LEN", "DHI", "PHM", "NVR", "MDC", "MTH", "TMHC", "LGIH",
    "TOL", "SKY", "CVCO",
    "BLD", "DOOR", "BECN", "IBP",
    "MMI", "COOP", "UWM", "PFSI",
    "CW", "TransDigm", "HEICO", "HEI", "HEI.A", "WWD",
    "DRS", "AXON", "RGR", "SWBI",
    "OSIS", "DXC", "CSC",
    "TSM", "ASML", "LSCC", "MTSI", "COHU", "KLIC", "FORM",
    "ENTG", "AEIS", "CAMT", "ACMR", "UCTT", "AMBA",
    "ALGM", "CRUS", "DIOD", "IXYS", "IXYS", "SMTC", "SIMO", "MKSI",
    "SITM", "POWI", "IMOS",
    "NUAN", "CDNA", "BPMC", "ALNY", "BMRN", "EXEL",
    "SGEN", "RGEN", "RARE", "ARWR", "SRPT", "MDGL",
    "ACAD", "ITCI", "INVA", "HALO", "ACLS", "NKTR",
    "KRTX", "TGTX", "RCKT", "PRTA", "IMGO",
    "EDIT", "NTLA", "BEAM", "CRSP", "VERV",
    "OHI", "SBRA", "LTC", "BRT", "IIPR",
    "CUZ", "HPP", "CTRE", "EXR", "CUBE", "NSA", "LSI",
    "WPC", "GOOD", "GTY", "LAND", "PINE",
    "USAC", "SMLP", "BSM", "TPVG",
    "MGLN", "MCHP", "TER", "ONTO", "IPGP", "NATI", "VI",
    "FARO", "IRTC", "ITRN", "COGT",
    "ADM", "BG", "CF", "MOS", "NTR", "LW", "SMPL",
    "INGR", "CALM", "HY", "JBSS",
    "ZBH", "SNN", "COO", "ATRC", "MASI",
    "HSKA", "ABMD", "NVCR", "AXNX",
    "TFX", "NUVA", "GMED", "NVT", "ESAB",
    "ATEX", "FMC", "ICL", "IFF",
    "AXTA", "RPM", "H.B. Fuller", "FUL", "Cabot", "CBT",
    "TRONC", "SLNL", "LEA", "ALV", "BWA", "DAN", "GT",
    "APTV", "DLPH", "GNTX", "DORMAN", "MOD", "MTOR", "STR", "SL",
    "WKHS", "RIDE", "GOEV", "CANOO", "FSR", "MULN",
    "CURV", "PTON", "LULU", "UA", "DECK", "SKX", "COLM", "VFC",
    "RL", "PVH", "HBI", "GIL", "HELE",
    "COTY", "REVG", "REV",
    "PLCE", "CPRI", "TPR", "WRBY",
    "GPC", "AAP", "AZO", "ORLY", "MNRO", "ALSN",
    "PII", "HOG", "BC", "PATK", "LCI",
    "NWHM", "MHO", "GRBK", "CSCO", "NTAP",
    "AGIO", "PTGX", "DNLI", "RCUS", "IMGO",
    "PRCT", "XELA", "CXW", "GEO", "PCOR",
    "ICF", "IYR", "VNQ",
    "GCI", "NXST", "SBGI", "TGNA", "AMCX", "PARA",
    "FOXA", "FOX", "WBD", "LUMN",
    "SIRI", "IHRT",
    "SONO", "VZIO", "VIEW",
    "MBI", "AMBAC", "AGO", "MBIA",
    "ARES", "KKR", "BXSL", "ARCC", "FSCO", "GBDC",
    "MAIN", "GAIN", "CSWC", "PFLT", "TPVG",
    "WES", "AM", "DT", "KMI", "MPLX", "ET", "PAA", "TRGP", "ENB",
    "EPD", "MMP", "BPL",
    "UGI", "NGL", "PBFX", "CAPL",
    "LBRT", "PTEN", "NBR", "RES", "WTTR", "PUMP", "ACDC",
    "CIVI", "SM", "MTDR", "CPE", "PDCE", "BATL", "ESTE",
    "VTLE", "FLMN", "RING",
    "GATO", "AG", "HL", "PAAS", "CDE", "MAG", "EXK",
    "NGD", "GPL", "USAS", "REX",
    "VALE", "RIO", "BHP", "GLNCY", "FCX", "NUE", "CLF",
    "STLD", "CMC", "SCHN", "RS",
    "AA", "CENX", "KALU", "CSTM",
    "CCJ", "UUUU", "DNN", "URG",
    "BWXT", "NLR",
    "LNG", "CLNE", "RUN", "NOVA", "MAXN",
    "AY", "CWEN", "BEP", "TERP",
    "WM", "RSG", "CVA", "GFL", "SRCL",
    "CLH", "HCCI", "US", "AQUA",
    "NTNX", "PCVX", "PNFP", "CBTX", "SMBC",
    "JHX", "CRHKY", "BNKCORP",
    "IMRE", "IMTX", "SEER",
    "TASK", "ONON", "GENI", "ACHR", "JOBY", "LILM",
    "SATL", "ASTS", "SPCE",
    "IONQ", "RGTI", "QUBT", "IQM",
    "SMAR", "ZI", "BRZE", "ALTR", "CFLT", "DOMO",
    "RAMP", "PUBM", "DV", "IAS",
    "MNTV", "SOUN", "BBAI", "GFAI", "SSYS",
    "XMTR", "DNLJ",
    "CIEN", "VIAV", "INFN", "ADTN", "CALX",
    "BAND", "LUMN", "GSAT",
    "CLSK", "CIFR", "WULF", "BTBT", "SDIG",
    "MARA", "RIOT", "HUT", "BITF",
    "DBRG", "INDT", "UNIT",
    "ARR", "AGNC", "TWO", "EFC", "MITT", "MFA",
    "BRMK", "GPMT", "KREF",
    "STWD", "BXMT", "LADR",
    "NRZ", "RITM", "NREF",
    "MPW", "GMRE", "CHCT", "PTON",
    "PSTL", "GOOD", "UNIT",
    "SVC", "CLNC", "FSCO",
    "BKDT", "DTM", "MGNI", "TTD",
    "MKTO", "PMTG", "LQDT",
    "SPSC", "EVBG", "SPRX",
    "TRUP", "BARK", "PETS",
    "SHOO", "GIII", "VRA", "BKE", "CATO",
    "PLBY", "JOANN", "HVT",
    "LAUR", "STRA", "PRDO", "CATO",
    "LRN", "APEI", "ARYE",
    "CLSD", "SABS", "KYMR",
]

# Deduplicate and clean the list
UNIVERSE_TICKERS = sorted(list(set([t for t in UNIVERSE_TICKERS if t and len(t) <= 5 and t.isalpha() or (len(t) <= 5 and all(c.isalpha() or c == '.' for c in t))])))

# Limit to ~1000 core tickers to keep it manageable
CORE_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "GOOG", "META", "TSLA", "AVGO", "JPM",
    "LLY", "V", "UNH", "XOM", "MA", "COST", "JNJ", "PG", "HD", "WMT",
    "MRK", "ABBV", "CVX", "BAC", "KO", "NFLX", "PEP", "CRM", "TMO", "ACN",
    "MCD", "AMD", "PM", "LIN", "CSCO", "GE", "ABT", "NOW", "DIS", "WFC",
    "CAT", "TXN", "NEE", "INTU", "IBM", "CMCSA", "ISRG", "SPGI", "GS", "AMGN",
    "QCOM", "RTX", "BKNG", "VZ", "HON", "LOW", "AMAT", "AXP", "PFE", "SCHW",
    "T", "UNP", "SYK", "BX", "MS", "UBER", "C", "DE", "MU", "BA",
    "ADI", "GILD", "ADP", "BLK", "ETN", "REGN", "PLD", "CB", "CI", "VRTX",
    "LRCX", "TJX", "MDLZ", "SO", "MMC", "EOG", "FI", "SHW", "BMY", "ICE",
    "CL", "PANW", "EQIX", "WM", "CME", "PH", "ZTS", "AON", "CARR", "KLAC",
    "MCO", "DUK", "SLB", "COF", "MAR", "TDG", "APH", "GEV", "APO", "MPC",
    "PSX", "WELL", "TT", "CTAS", "GD", "HCA", "OXY", "PCAR", "SNPS", "ECL",
    "CDNS", "CMG", "NOC", "MSI", "NKE", "FTNT", "ORLY", "CEG", "AFL", "AIG",
    "PSA", "WDAY", "TMUS", "WMB", "EW", "HES", "COP", "AZO", "FANG",
    "PCG", "KMB", "ALL", "STZ", "RCL", "CBRE", "ITW", "D", "PPG", "DOW",
    "LHX", "PAYX", "HSY", "EXC", "GWW", "CCI", "FICO", "PEG", "HAL", "BIIB",
    "FAST", "ACGL", "VRSK", "BDX", "KEYS", "HLT", "KHC", "RSG", "CTSH", "NEM",
    "A", "OKE", "XEL", "DAL", "UAL", "F", "GM", "SBUX",
    "EBAY", "PYPL", "SQ", "SNOW", "PLTR", "CRWD", "DDOG", "NET", "ZS", "OKTA",
    "HUBS", "BILL", "NTNX", "PTC", "MANH", "PAYC", "PCTY", "WK", "CDAY",
    "CLX", "CHD", "EL", "ULTA", "ELF", "COTY",
    "TAP", "MNST", "CELH",
    "CAG", "SJM", "CPB", "MKC", "HRL", "TSN",
    "WH", "CHH", "H", "IHG",
    "EXP", "MLM", "VMC", "IP", "AVY", "PKG",
    "DLR", "AMT", "IRM", "ARE", "BXP", "EQR", "AVB",
    "ESS", "MAA", "UDR", "NNN", "O", "VICI", "GLPI", "STAG",
    "AJG", "WTW", "MKL", "RE",
    "OC", "SWK", "MAS", "TREX",
    "LII", "ROK", "EMR", "TRMB",
    "DXCM", "PODD", "ALGN", "HOLX", "BIO", "XRAY",
    "MCK", "CAH", "COR", "HSIC",
    "HUM", "MOH", "CNC", "ELV", "ANTM",
    "IQV", "ICLR", "MEDP",
    "RMD",
    "AMP", "PFG", "VOYA", "BEN", "IVZ", "AMG",
    "NTRS", "STT", "TROW", "KEY", "CMA", "ZION",
    "FITB", "HBAN", "RF", "CFG", "ALLY", "SYF",
    "DFS", "GPN", "FIS", "FLT", "WEX",
    "MTB", "EWBC", "WAL",
    "GL", "MET", "PRU",
    "AEP", "FE", "ES", "AEE", "CMS", "LNT", "NI", "PNW", "EVRG", "ATO",
    "AWK",
    "LEN", "DHI", "PHM", "NVR", "MDC", "MTH",
    "BLD", "DOOR", "BECN",
    "CW", "HEICO", "HEI",
    "AXON",
    "TSM", "ASML", "LSCC", "ENTG", "AMBA",
    "ALGM", "CRUS", "DIOD",
    "ALNY", "BMRN", "EXEL",
    "SGEN", "RGEN", "RARE", "ARWR", "SRPT",
    "ADM", "BG", "CF", "MOS", "NTR", "LW",
    "ZBH", "COO",
    "TFX", "NUVA", "GMED",
    "FMC", "IFF",
    "AXTA", "RPM", "FUL", "CBT",
    "LEA", "ALV", "BWA", "GT",
    "APTV", "GNTX",
    "LULU", "UA", "DECK", "SKX", "COLM",
    "RL", "PVH", "HBI", "GIL",
    "GPC", "AAP", "MNRO",
    "PII", "HOG", "BC",
    "NXST", "TGNA", "AMCX", "PARA",
    "FOXA", "FOX", "WBD", "LUMN",
    "SIRI",
    "ARES", "KKR", "ARCC",
    "MAIN",
    "WES", "AM", "KMI", "MPLX", "ET", "PAA", "TRGP",
    "EPD",
    "LBRT", "PTEN",
    "CIVI", "SM", "MTDR", "CPE",
    "VALE", "RIO", "BHP", "FCX", "NUE", "CLF",
    "STLD", "CMC", "RS",
    "AA",
    "CCJ",
    "WM", "RSG", "CLH",
    "CIEN", "VIAV", "INFN", "CALX",
    "MARA", "RIOT",
    "AGNC", "TWO",
    "STWD", "BXMT",
    "MPW",
    "TTD",
    "TRUP",
    "SHOO",
    "CLSD",
    "ONON",
    "JOBY",
    "IONQ",
    "ZI", "BRZE", "ALTR", "CFLT",
    "RAMP",
    "SOUN",
    "ABBV", "REGN", "VRTX", "BIIB", "AMGN", "GILD",
    "BSX", "MDT", "STE", "VAR", "NVCR",
    "HUM", "WBA", "RAD", "CVS",
    "POOL", "SNA", "MSA", "GNSS",
    "MPWR", "WOLF", "ON", "MRVL", "SWKS", "QRVO",
    "NVST", "HOLX", "PODD", "DXCM",
    "ACM", "PWR", "MTZ",
    "PNR", "IR", "TDY", "LDOS", "BAH", "SAIC",
    "IEX",
    "ROST", "BURL", "FIVE", "OLLI", "DG", "DLTR", "KR", "SFM",
    "ZM", "DOCU", "TWLO",
    "CLX", "EL",
    "SONO",
    "BWXT",
    "LNG", "RUN",
    "AY", "BEP",
    "SRCL",
    "AQUA",
    "TASK", "GENI", "ACHR",
    "SATL", "ASTS",
    "RGTI", "QUBT",
    "SMAR", "DOMO",
    "PUBM", "DV",
    "BBAI", "SSYS",
    "NTAP", "HPQ", "HPE", "DELL", "SMCI", "PSTG",
    "ANET", "JNPR",
    "INTC",
    "GPK", "SEE", "BERY", "SLVM",
    "FDS", "MSCI", "NDAQ", "ICE",
    "IAC", "IAG",
    "GDDY", "AKAM", "FSLY",
    "ESTC", "TLND",
    "PSO", "CHGG", "2U",
    "BFLY", "ACQR", "DTIL",
    "CELC", "NTLA", "BEAM",
    "EDIT", "CRSP", "VERV",
    "IMGN", "FATE", "BLUE",
    "KYMR",
    "PG", "CLX", "CHD", "HRL",
    "ODFL", "JBHT", "CHRW", "XPO", "SAIA", "ARCB",
    "FDX", "UPS",
    "EXPD", "ECHO",
    "ZTO", "GXO",
    "PLUG", "BE", "BLOOM", "EVGO", "CHPT",
    "BLNK", "WKHS",
    "VG", "CHWY", "W", "ETSY", "VSCO",
    "PRTS", "FTCH",
    "MDB", "COUCHBASE", "NEWR", "SUMO", "ESTC",
    "VRNS", "VNET", "TTEC",
    "PRFT", "EPAM", "GLOB", "GCBC",
    "CNXN", "PCTI", "CSGS",
    "MFIN", "STEL", "CIVB",
    "COOP", "UWM", "PFSI", "NMIH",
    "FBP", "OFG", "BPOP",
    "VBTX", "SFNC", "HMNF",
    "MATX", "ESEA", "GOGL",
    "ZIM", "ATCO", "DAC",
    "CEVA", "CMPR", "INFOB",
    "SMTC", "DIOD", "IXYS",
    "LPRO", "OPEN", "TREE",
    "PFBF", "SIF",
    "ARHS", "BCRX", "IMUX",
    "RXDX", "IPIX", "THTX",
    "AMRN", "TLRA", "DRRX",
    "CRBP", "BPMC", "AGIO",
    "PTGX", "DNLI",
    "ATRA",
    "BHVN", "TSVT",
    "IMTX", "KRTX",
    "CLOV", "TDOC", "AMWL",
    "HIMS", "GDRX",
    "TW", "SPSC",
    "NEOG",
    "PAG", "AN", "SAH", "ABG", "LAD",
    "CAR", "HTZ", "RCLCO",
    "ARMK", "ABM",
    "WERN", "MRTN", "HTLD",
    "KNSL", "RELI", "GBNY",
    "WBS", "PNFP", "BOKF",
    "WAFD", "UMPQ", "SRCE",
    "HWC", "BKU", "CBTX",
    "STBA", "FFBC", "MBWM",
    "RKT", "PFNL", "BKD",
    "PPC", "JBSAY", "HFC",
    "DAR", "SEB", "TDC",
    "BGS", "SENEA", "JJSF",
    "CORE", "TWNK", "UTZ",
    "DCOMP", "PFMB",
    "MHO", "GRBK", "CSCO", "NTAP",
    "TREX", "FBHS",
    "AZEK",
    "TRU", "VRSK", "EXPN",
    "FDS", "MSCI",
    "CBOE",
    "NDAQ",
    "MKTX",
    "EVR", "PJT", "LAZ",
    "JEF",
    "IBKR",
    "RJF",
    "LPL",
    "LPLA",
    "VIRT",
    "HOOD",
    "NRDS",
]

# Deduplicate core tickers
CORE_TICKERS = sorted(list(set([t for t in CORE_TICKERS if t and len(t) <= 5 and t.replace('.', '').isalpha()])))

# Global data store
_price_data: Optional[pd.DataFrame] = None
_meta_data: Optional[dict] = None
_rankings_cache: Optional[dict] = None
_last_params: Optional[dict] = None


def update_status(status: str, message: str, progress: float = 0, total: int = 0, loaded: int = 0):
    with _status_lock:
        _status.update({
            "status": status,
            "message": message,
            "progress": progress,
            "total": total,
            "loaded": loaded,
        })
        if status == "ready":
            _status["cached_at"] = datetime.now().isoformat()


def get_status() -> dict:
    with _status_lock:
        return dict(_status)


def winsorize(series: pd.Series, p: float = 2.0) -> pd.Series:
    lo = np.percentile(series.dropna(), p)
    hi = np.percentile(series.dropna(), 100 - p)
    return series.clip(lo, hi)


def zscore(series: pd.Series) -> pd.Series:
    mu = series.mean()
    sigma = series.std()
    if sigma < 1e-10:
        return series * 0
    return (series - mu) / sigma


def ols_tstat(log_prices: pd.Series) -> float:
    """OLS t-stat of log price vs time."""
    n = len(log_prices)
    if n < 10:
        return np.nan
    x = np.arange(n, dtype=float)
    x -= x.mean()
    y = log_prices.values
    y = np.where(np.isfinite(y), y, np.nan)
    mask = ~np.isnan(y)
    if mask.sum() < 10:
        return np.nan
    x2 = x[mask]
    y2 = y[mask]
    slope, intercept, r_value, p_value, std_err = stats.linregress(x2, y2)
    if std_err < 1e-10:
        return np.nan
    return slope / std_err


def load_data_batch(tickers: list, batch_size: int = 50) -> tuple:
    """Download price data in batches."""
    all_close = {}
    failed = []

    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    total_batches = len(batches)

    update_status("loading", f"Downloading price data (0/{len(tickers)} stocks)...",
                  progress=0, total=len(tickers), loaded=0)

    for bi, batch in enumerate(batches):
        try:
            raw = yf.download(
                batch,
                period="2y",
                auto_adjust=True,
                progress=False,
                timeout=60,
            )

            if raw.empty:
                failed.extend(batch)
                continue

            # Extract Close prices
            if isinstance(raw.columns, pd.MultiIndex):
                close = raw["Close"]
            else:
                close = raw[["Close"]] if "Close" in raw.columns else raw

            for ticker in batch:
                try:
                    if ticker in close.columns:
                        col = close[ticker].dropna()
                        if len(col) >= 252:
                            all_close[ticker] = col
                        else:
                            failed.append(ticker)
                    else:
                        failed.append(ticker)
                except Exception:
                    failed.append(ticker)

        except Exception as e:
            logger.error(f"Batch {bi} error: {e}")
            failed.extend(batch)

        loaded = sum(len(v) > 0 for v in all_close.values())
        progress = (bi + 1) / total_batches
        update_status("loading", f"Downloading price data ({loaded}/{len(tickers)} stocks)...",
                      progress=progress, total=len(tickers), loaded=loaded)

    return all_close, failed


def load_meta_batch(tickers: list) -> dict:
    """Load metadata (sector, market cap, etc.) for tickers."""
    meta = {}
    update_status("loading", "Loading stock metadata...", progress=0.8)

    batch_size = 20
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        for ticker in batch:
            try:
                info = yf.Ticker(ticker).fast_info
                meta[ticker] = {
                    "name": getattr(info, "description", ticker) or ticker,
                    "sector": None,
                    "industry": None,
                    "market_cap": getattr(info, "market_cap", None),
                    "price": getattr(info, "last_price", None),
                    "currency": getattr(info, "currency", "USD"),
                }
            except Exception:
                meta[ticker] = {
                    "name": ticker,
                    "sector": None,
                    "industry": None,
                    "market_cap": None,
                    "price": None,
                    "currency": "USD",
                }

    return meta


def load_meta_with_info(tickers: list) -> dict:
    """Load richer metadata including sector, ROE, ROA etc."""
    meta = {}

    for i, ticker in enumerate(tickers):
        if i % 20 == 0:
            progress = 0.8 + (i / len(tickers)) * 0.15
            update_status("loading", f"Loading metadata ({i}/{len(tickers)})...", progress=progress)
        try:
            t = yf.Ticker(ticker)
            info = t.info

            roe = info.get("returnOnEquity")
            roa = info.get("returnOnAssets")
            gross_margin = info.get("grossMargins")
            op_margin = info.get("operatingMargins")
            de_ratio = info.get("debtToEquity")

            meta[ticker] = {
                "name": info.get("longName") or info.get("shortName") or ticker,
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": info.get("marketCap"),
                "price": info.get("currentPrice") or info.get("regularMarketPrice"),
                "avg_volume": info.get("averageDailyVolume10Day"),
                "roe": roe,
                "roa": roa,
                "gross_margin": gross_margin,
                "op_margin": op_margin,
                "de_ratio": de_ratio,
            }
        except Exception:
            meta[ticker] = {
                "name": ticker,
                "sector": None,
                "industry": None,
                "market_cap": None,
                "price": None,
                "avg_volume": None,
                "roe": None,
                "roa": None,
                "gross_margin": None,
                "op_margin": None,
                "de_ratio": None,
            }

    return meta


def compute_factors(prices: pd.DataFrame, meta: dict,
                    vol_floor: float = 0.05,
                    winsor_p: float = 2.0,
                    use_tstats: bool = False) -> pd.DataFrame:
    """Compute all factor scores for each stock."""
    log_prices = np.log(prices)
    log_returns = log_prices.diff()

    rows = []
    for ticker in prices.columns:
        lp = log_prices[ticker].dropna()
        lr = log_returns[ticker].dropna()

        n = len(lp)
        if n < 252:
            continue

        p_now = lp.iloc[-1]

        # Momentum
        r1 = lp.iloc[-1] - lp.iloc[-22] if n >= 22 else np.nan
        r6 = lp.iloc[-1] - lp.iloc[-127] if n >= 127 else np.nan
        r12 = lp.iloc[-1] - lp.iloc[-253] if n >= 253 else np.nan

        m6 = r6 - r1 if (not np.isnan(r6) and not np.isnan(r1)) else np.nan
        m12 = r12 - r1 if (not np.isnan(r12) and not np.isnan(r1)) else np.nan

        # Volatility
        sigma6 = lr.iloc[-126:].std() * np.sqrt(252) if n >= 126 else np.nan
        sigma12 = lr.std() * np.sqrt(252)

        # Apply vol floor
        sigma6_adj = max(sigma6, vol_floor) if not np.isnan(sigma6) else vol_floor
        sigma12_adj = max(sigma12, vol_floor)

        # Sharpe-adjusted
        s6 = m6 / sigma6_adj if not np.isnan(m6) else np.nan
        s12 = m12 / sigma12_adj if not np.isnan(m12) else np.nan

        # OLS t-stats
        tstat6 = np.nan
        tstat12 = np.nan
        if use_tstats:
            tstat6 = ols_tstat(lp.iloc[-126:]) if n >= 126 else np.nan
            tstat12 = ols_tstat(lp) if n >= 252 else np.nan

        # Quality from meta
        m = meta.get(ticker, {})
        roe = m.get("roe")
        roa = m.get("roa")
        gross_margin = m.get("gross_margin")
        op_margin = m.get("op_margin")
        de_ratio = m.get("de_ratio")

        profitability = roe if roe is not None else roa
        margin = gross_margin if gross_margin is not None else op_margin
        leverage = -de_ratio if de_ratio is not None else None  # lower leverage is better, so negate

        # Market cap and ADV
        market_cap = m.get("market_cap")
        price_last = np.exp(p_now)
        avg_vol_shares = m.get("avg_volume")
        adv = price_last * avg_vol_shares if avg_vol_shares else None

        rows.append({
            "ticker": ticker,
            "name": m.get("name", ticker),
            "sector": m.get("sector"),
            "industry": m.get("industry"),
            "price": price_last,
            "market_cap": market_cap,
            "adv": adv,
            "r1": r1,
            "m6": m6,
            "m12": m12,
            "sigma6": sigma6,
            "sigma12": sigma12,
            "s6": s6,
            "s12": s12,
            "tstat6": tstat6,
            "tstat12": tstat12,
            "_profitability": profitability,
            "_margin": margin,
            "_leverage": leverage,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Compute quality composite
    quality_parts = []
    for col in ["_profitability", "_margin", "_leverage"]:
        series = df[col].copy()
        valid = series.notna()
        if valid.sum() > 10:
            series[valid] = zscore(winsorize(series[valid], winsor_p))
            quality_parts.append(series)

    if quality_parts:
        q_df = pd.concat(quality_parts, axis=1)
        df["quality"] = q_df.mean(axis=1)
    else:
        df["quality"] = np.nan

    return df


def compute_rankings(df: pd.DataFrame,
                     vol_adjust: bool = True,
                     use_quality: bool = True,
                     use_tstats: bool = False,
                     w6: float = 0.4,
                     w12: float = 0.4,
                     w_quality: float = 0.2,
                     winsor_p: float = 2.0,
                     vol_floor: float = 0.05,
                     cluster_n: int = 100,
                     cluster_k: int = 10,
                     cluster_lookback: int = 252,
                     prices: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Apply standardization and compute alpha scores."""
    if df.empty:
        return df

    d = df.copy()

    def std_factor(series):
        v = series[series.notna()]
        if len(v) < 10:
            return series * 0
        out = series.copy()
        out[out.notna()] = zscore(winsorize(v, winsor_p))
        return out

    # Choose momentum factor
    if vol_adjust:
        d["_f6"] = std_factor(d["s6"])
        d["_f12"] = std_factor(d["s12"])
    else:
        d["_f6"] = std_factor(d["m6"])
        d["_f12"] = std_factor(d["m12"])

    d["zM6"] = d["_f6"]
    d["zM12"] = d["_f12"]

    if use_quality:
        d["zQuality"] = std_factor(d["quality"])
    else:
        d["zQuality"] = 0.0

    # Normalize weights
    total_w = w6 + w12 + (w_quality if use_quality else 0)
    if total_w <= 0:
        total_w = 1.0

    alpha_parts = (w6 * d["zM6"].fillna(0) + w12 * d["zM12"].fillna(0))
    if use_quality:
        alpha_parts += w_quality * d["zQuality"].fillna(0)
    d["alpha"] = alpha_parts / total_w

    # Rank
    d = d.sort_values("alpha", ascending=False)
    d["rank"] = np.arange(1, len(d) + 1)
    d["percentile"] = 100.0 * (1 - (d["rank"] - 1) / len(d))

    # Clustering
    d["cluster"] = None
    if prices is not None and len(d) > 0:
        top_n = d.head(cluster_n)["ticker"].tolist()
        valid_tickers = [t for t in top_n if t in prices.columns]
        if len(valid_tickers) >= cluster_k:
            try:
                sub = prices[valid_tickers].tail(cluster_lookback)
                lr = sub.pct_change().dropna()
                if lr.shape[0] > 30:
                    corr = lr.corr().fillna(0).clip(-1, 1)
                    dist = 1 - corr
                    dist = np.clip(dist.values, 0, 2)
                    clust = AgglomerativeClustering(n_clusters=min(cluster_k, len(valid_tickers)),
                                                    metric="precomputed",
                                                    linkage="average")
                    labels = clust.fit_predict(dist)
                    label_map = dict(zip(valid_tickers, labels.tolist()))
                    d["cluster"] = d["ticker"].map(label_map)
            except Exception as e:
                logger.error(f"Clustering error: {e}")

    return d


def apply_universe_filters(df: pd.DataFrame,
                           min_price: float = 5.0,
                           min_adv: float = 1e7,
                           min_market_cap: float = 1e9) -> pd.DataFrame:
    """Apply basic universe filters."""
    mask = pd.Series(True, index=df.index)
    if "price" in df.columns:
        mask &= df["price"].fillna(0) >= min_price
    if "adv" in df.columns:
        mask &= df["adv"].fillna(0) >= min_adv
    if "market_cap" in df.columns:
        mask &= df["market_cap"].fillna(0) >= min_market_cap
    return df[mask].copy()


def get_price_data():
    return _price_data


def get_meta_data():
    return _meta_data


def get_ranked_data(params: dict) -> Optional[pd.DataFrame]:
    global _rankings_cache, _last_params, _price_data, _meta_data
    if _price_data is None:
        return None

    cache_key = json.dumps(params, sort_keys=True)
    if _rankings_cache is not None and _last_params == cache_key:
        return _rankings_cache

    factors = compute_factors(
        _price_data, _meta_data,
        vol_floor=params.get("vol_floor", 0.05),
        winsor_p=params.get("winsor_p", 2.0),
        use_tstats=params.get("use_tstats", False),
    )

    if factors.empty:
        return factors

    factors = apply_universe_filters(factors)

    ranked = compute_rankings(
        factors,
        vol_adjust=params.get("vol_adjust", True),
        use_quality=params.get("use_quality", True),
        use_tstats=params.get("use_tstats", False),
        w6=params.get("w6", 0.4),
        w12=params.get("w12", 0.4),
        w_quality=params.get("w_quality", 0.2),
        winsor_p=params.get("winsor_p", 2.0),
        vol_floor=params.get("vol_floor", 0.05),
        cluster_n=params.get("cluster_n", 100),
        cluster_k=params.get("cluster_k", 10),
        cluster_lookback=params.get("cluster_lookback", 252),
        prices=_price_data,
    )

    _rankings_cache = ranked
    _last_params = cache_key
    return ranked


def compute_portfolio_risk(tickers: list, weights: list, lookback: int = 252) -> dict:
    """Compute portfolio risk metrics."""
    global _price_data

    if _price_data is None:
        return {"error": "Data not loaded"}

    valid = [(t, w) for t, w in zip(tickers, weights) if t in _price_data.columns]
    if not valid:
        return {"error": "No valid tickers in price data"}

    tickers_v = [x[0] for x in valid]
    weights_v = np.array([x[1] for x in valid])
    weights_v = weights_v / weights_v.sum()

    prices_sub = _price_data[tickers_v].tail(lookback)
    log_returns = np.log(prices_sub / prices_sub.shift(1)).dropna()

    # Individual vols
    vols = log_returns.std() * np.sqrt(252)

    # Covariance matrix
    cov = log_returns.cov() * 252

    # Portfolio vol
    port_vol = np.sqrt(weights_v @ cov.values @ weights_v)

    # Correlation
    corr = log_returns.corr()
    n = len(tickers_v)
    if n > 1:
        mask = np.ones((n, n), dtype=bool)
        np.fill_diagonal(mask, False)
        avg_corr = corr.values[mask].mean()
    else:
        avg_corr = 1.0

    return {
        "port_vol": float(port_vol),
        "avg_corr": float(avg_corr),
        "vols": {t: float(vols[t]) for t in tickers_v},
        "weights": {t: float(w) for t, w in zip(tickers_v, weights_v)},
    }


def initial_data_load():
    """Background thread: load all price and meta data."""
    global _price_data, _meta_data

    # Check cache
    cache_key = "price_data_v3"
    cached = cache.get(cache_key)
    if cached:
        logger.info("Loading from disk cache")
        update_status("loading", "Restoring from cache...", progress=0.5)
        try:
            _price_data, _meta_data = cached
            update_status("ready", f"Ready. {len(_price_data.columns)} stocks loaded from cache.",
                          progress=1.0, total=len(_price_data.columns), loaded=len(_price_data.columns))
            return
        except Exception:
            pass

    tickers = CORE_TICKERS
    logger.info(f"Loading data for {len(tickers)} tickers")
    update_status("loading", f"Starting download for {len(tickers)} stocks...",
                  progress=0, total=len(tickers), loaded=0)

    # Download prices in batches
    all_close, failed = load_data_batch(tickers, batch_size=50)

    if not all_close:
        update_status("error", "Failed to load any price data. Check network connectivity.")
        return

    # Build price dataframe aligned to trading calendar
    dfs = []
    for ticker, series in all_close.items():
        s = series.rename(ticker)
        dfs.append(s)

    prices = pd.concat(dfs, axis=1)
    prices = prices.sort_index()
    prices = prices.dropna(how="all")

    # Forward fill gaps (up to 5 days)
    prices = prices.fillna(method="ffill", limit=5)

    # Filter: keep tickers with >= 252 days
    valid_cols = [c for c in prices.columns if prices[c].count() >= 252]
    prices = prices[valid_cols]

    logger.info(f"Price data: {prices.shape}, {len(failed)} failed")

    # Load meta data
    update_status("loading", f"Loading sector and quality data for {len(valid_cols)} stocks...",
                  progress=0.75, total=len(valid_cols), loaded=len(valid_cols))

    meta = load_meta_with_info(valid_cols)

    _price_data = prices
    _meta_data = meta

    # Cache to disk
    try:
        cache.set(cache_key, (prices, meta), expire=CACHE_TTL)
    except Exception as e:
        logger.error(f"Cache write failed: {e}")

    update_status("ready", f"Ready. {len(valid_cols)} stocks loaded.",
                  progress=1.0, total=len(valid_cols), loaded=len(valid_cols))
    logger.info("Data load complete")


def start_background_load():
    """Start data loading in background thread."""
    t = threading.Thread(target=initial_data_load, daemon=True)
    t.start()
