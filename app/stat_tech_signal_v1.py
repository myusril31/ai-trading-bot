import json, math, os, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

MARKER = "STAT_TECH_V1_BALANCED_SHADOW_20260628"


def now_iso(): return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
def envf(k,d):
    try: return float(os.getenv(k, d))
    except Exception: return d
def envi(k,d):
    try: return int(float(os.getenv(k, d)))
    except Exception: return d
def csv(k,d=""):
    return [x.strip().upper() for x in str(os.getenv(k,d) or "").split(",") if x.strip()]
def log_dir(): return Path(os.getenv("LOG_DIR", "logs"))
def state_dir(): return Path(os.getenv("STATE_DIR", "state"))
def market_dir(): return Path(os.getenv("BINANCE_CANDLE_STORE_DIR") or os.getenv("MARKET_DATA_DIR") or state_dir()/"market_data")
def append_jsonl(path: Path, row: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f: f.write(json.dumps(row, ensure_ascii=False, separators=(",",":"))+"\n")

def symbols_default():
    return sorted(set(csv("PAIR_ALLOWLIST") or "ADAUSDT,AVAXUSDT,BCHUSDT,BTCUSDT,ETHUSDT,HYPEUSDT,LINKUSDT,LTCUSDT,PAXGUSDT,SOLUSDT,SUIUSDT,UNIUSDT,XRPUSDT,ZECUSDT".split(",")))

def fnum(x, d=None):
    try:
        v=float(x)
        return d if math.isnan(v) or math.isinf(v) else v
    except Exception: return d

def load_candles(symbol: str, interval: str, limit: int=240) -> List[Dict[str, Any]]:
    p = market_dir()/f"{symbol.upper()}_{interval}.jsonl"
    out=[]
    if not p.exists(): return out
    for line in p.open("r", encoding="utf-8", errors="ignore"):
        try: r=json.loads(line)
        except Exception: continue
        if os.getenv("STAT_TECH_USE_CLOSED_CANDLES_ONLY","true").lower() in ("1","true","yes") and not bool(r.get("is_closed", False)): continue
        try:
            o,h,l,c,v = float(r["open"]),float(r["high"]),float(r["low"]),float(r["close"]),float(r["volume"])
            qv = fnum(r.get("quote_volume"), c*v) or 0.0
            out.append({"t":int(r.get("close_time_ms") or 0),"ot":int(r.get("open_time_ms") or 0),"o":o,"h":h,"l":l,"c":c,"v":v,"qv":qv})
        except Exception: continue
    return sorted(out, key=lambda x:x["ot"])[-limit:]

def closes(a): return [x["c"] for x in a]
def ema(vals, n):
    if not vals: return []
    k=2/(n+1); out=[]; e=None
    for v in vals:
        e = v if e is None else v*k + e*(1-k)
        out.append(e)
    return out

def sma(vals,n):
    return [None if i+1<n else sum(vals[i+1-n:i+1])/n for i in range(len(vals))]

def std(vals,n):
    out=[]
    for i in range(len(vals)):
        if i+1<n: out.append(None); continue
        w=vals[i+1-n:i+1]; m=sum(w)/len(w)
        out.append(math.sqrt(sum((x-m)**2 for x in w)/len(w)))
    return out

def tr_list(a):
    out=[]; pc=None
    for r in a:
        tr = r["h"]-r["l"] if pc is None else max(r["h"]-r["l"], abs(r["h"]-pc), abs(r["l"]-pc))
        out.append(max(tr,0)); pc=r["c"]
    return out

def atr(a,n=14): return sma(tr_list(a),n)
def pctile(vals, cur=None):
    x=[v for v in vals if v is not None]
    if not x: return None
    c=x[-1] if cur is None else cur
    return 100*sum(1 for v in x if v<=c)/len(x)
def zscore(vals,n=50):
    x=[v for v in vals if v is not None]
    if len(x)<max(10,n//3): return None
    w=x[-n:]; m=sum(w)/len(w); s=math.sqrt(sum((v-m)**2 for v in w)/len(w))
    return 0.0 if s<=1e-12 else (x[-1]-m)/s

def adx(a,n=14):
    if len(a)<n+2: return None
    pdm=[]; mdm=[]; trs=[]
    for p,r in zip(a[:-1], a[1:]):
        up=r["h"]-p["h"]; dn=p["l"]-r["l"]
        pdm.append(up if up>dn and up>0 else 0); mdm.append(dn if dn>up and dn>0 else 0)
        trs.append(max(r["h"]-r["l"], abs(r["h"]-p["c"]), abs(r["l"]-p["c"])))
    ts=sum(trs[-n:])
    if ts<=0: return None
    pdi=100*sum(pdm[-n:])/ts; mdi=100*sum(mdm[-n:])/ts; den=pdi+mdi
    return None if den<=0 else 100*abs(pdi-mdi)/den

def vwap(a,n=48):
    w=a[-n:]; num=den=0.0
    for r in w:
        typ=(r["h"]+r["l"]+r["c"])/3; weight=r.get("qv") or r["v"]
        num += typ*weight; den += weight
    return num/den if den>0 else None

def donchian(a,n=20,ex=True):
    b=a[:-1] if ex else a
    if len(b)<n: return None,None,None
    w=b[-n:]; hi=max(r["h"] for r in w); lo=min(r["l"] for r in w)
    return hi,lo,hi-lo

def ret(a,bars):
    if len(a)<=bars or a[-bars-1]["c"]<=0: return None
    return a[-1]["c"]/a[-bars-1]["c"]-1

def volume_z(a): return zscore([r["v"] for r in a],50)
def taker_proxy(a,n=12):
    w=a[-n:]; buy=tot=0.0
    for r in w:
        rng=max(r["h"]-r["l"],1e-12); pos=(r["c"]-r["l"])/rng
        buy += r["v"]*max(0,min(1,pos)); tot += r["v"]
    return None if tot<=0 else (buy/tot-0.5)*2

def atr_pct(a):
    x=[v for v in atr(a,14) if v is not None]
    return pctile(x[-120:]) if len(x)>=20 else None
def bb_pct(a):
    c=closes(a); m=sma(c,20); s=std(c,20); widths=[]
    for cc,mm,ss in zip(c,m,s):
        if mm and ss is not None and mm>0: widths.append((4*ss)/mm)
    return pctile(widths[-120:]) if len(widths)>=20 else None
def range_pct(a,n=20):
    widths=[]
    for i in range(n,len(a)+1):
        w=a[i-n:i]; cc=w[-1]["c"]
        if cc>0: widths.append((max(r["h"] for r in w)-min(r["l"] for r in w))/cc)
    return pctile(widths[-120:]) if len(widths)>=20 else None

def rel_context(symbols):
    rets={}
    for s in symbols:
        a=load_candles(s, os.getenv("STAT_TECH_INTERVAL_ENTRY","15m"), 120)
        r=ret(a,envi("STAT_TECH_REL_RETURN_BARS",16)) if a else None
        if r is not None: rets[s]=r
    if not rets: return {}
    vals=list(rets.values()); m=sum(vals)/len(vals); sd=math.sqrt(sum((x-m)**2 for x in vals)/len(vals)) or 1e-12; btc=rets.get("BTCUSDT",m)
    ranked=sorted(rets.items(), key=lambda kv:kv[1], reverse=True); n=len(ranked); out={}
    for i,(s,r) in enumerate(ranked,1): out[s]={"rank":i,"pctile":100*(n-i+1)/n,"vs_btc_z":(r-btc)/sd,"vs_uni_z":(r-m)/sd,"ret":r}
    return out

def features(symbol, rel):
    h=load_candles(symbol, os.getenv("STAT_TECH_INTERVAL_HTF","4h"), 220)
    e=load_candles(symbol, os.getenv("STAT_TECH_INTERVAL_ENTRY","15m"), 220)
    t=load_candles(symbol, os.getenv("STAT_TECH_INTERVAL_TRIGGER","5m"), 220)
    if len(h)<60 or len(e)<80 or len(t)<50: return None,{"htf":len(h),"entry":len(e),"trigger":len(t)}
    c4=closes(h); c15=closes(e); c5=closes(t); ema50_4=ema(c4,50); ema20_15=ema(c15,20); ema50_15=ema(c15,50); ema20_5=ema(c5,20)
    a15=[x for x in atr(e,14) if x is not None]; atr15=a15[-1] if a15 else None; vw=vwap(e,48); last=e[-1]["c"]
    vwdevs=[]
    for i in range(max(10,len(e)-80),len(e)):
        vv=vwap(e[:i+1],48)
        if vv and vv>0: vwdevs.append((e[i]["c"]-vv)/vv)
    hi,lo,w=donchian(e,20,True); r=rel.get(symbol,{})
    slope4=(ema50_4[-1]/ema50_4[-6]-1) if len(ema50_4)>=6 and ema50_4[-6] else 0
    slope15=(ema50_15[-1]/ema50_15[-6]-1) if len(ema50_15)>=6 and ema50_15[-6] else 0
    return {"last":last,"last5":t[-1]["c"],"ema50_4h":ema50_4[-1],"ema50_slope_4h":slope4,"ema20_15m":ema20_15[-1],"ema50_15m":ema50_15[-1],"ema50_slope_15m":slope15,"ema20_5m":ema20_5[-1],"adx_4h":adx(h),"adx_15m":adx(e),"atr_15m":atr15,"atr_pctile_15m":atr_pct(e),"bb_width_pctile_15m":bb_pct(e),"range_width_pctile_15m":range_pct(e),"vwap_15m":vw,"vwap_dev_z_15m":zscore(vwdevs,50),"volume_z_15m":volume_z(e),"taker_imbalance_15m":taker_proxy(e),"donchian_high_15m":hi,"donchian_low_15m":lo,"swing_low_15m":min(x["l"] for x in e[-12:]),"swing_high_15m":max(x["h"] for x in e[-12:]),"return_15m_1h":ret(e,4),"return_5m_15m":ret(t,3),"pair_vs_btc_return_z":r.get("vs_btc_z"),"relative_strength_rank":r.get("rank"),"relative_strength_pctile":r.get("pctile")}, None

def regime(f):
    if (f.get("atr_pctile_15m") or 0)>=envf("STAT_TECH_HIGH_VOL_ATR_PCTILE",92): return "HIGH_VOL"
    if (f.get("bb_width_pctile_15m") or 100)<envf("STAT_TECH_COMPRESSION_BB_PCTILE",25) and (f.get("range_width_pctile_15m") or 100)<envf("STAT_TECH_COMPRESSION_RANGE_PCTILE",30): return "COMPRESSION"
    if f["last"]>f["ema50_4h"] and f["ema50_slope_4h"]>envf("STAT_TECH_TREND_SLOPE_MIN",0.001) and (f.get("adx_4h") or 0)>=envf("STAT_TECH_ADX_TREND_MIN",14): return "TREND_UP"
    if f["last"]<f["ema50_4h"] and f["ema50_slope_4h"]<-envf("STAT_TECH_TREND_SLOPE_MIN",0.001) and (f.get("adx_4h") or 0)>=envf("STAT_TECH_ADX_TREND_MIN",14): return "TREND_DOWN"
    if (f.get("adx_15m") or 99)<envf("STAT_TECH_RANGE_ADX_MAX",18): return "RANGE"
    return "NEUTRAL"

def direction(f, rg):
    l=s=0
    if f["last"]>f["ema50_15m"]: l+=2
    if f["last"]<f["ema50_15m"]: s+=2
    if f["ema50_slope_15m"]>0.0005: l+=1
    if f["ema50_slope_15m"]<-0.0005: s+=1
    if (f.get("return_15m_1h") or 0)>0.002: l+=1
    if (f.get("return_15m_1h") or 0)<-0.002: s+=1
    ti=f.get("taker_imbalance_15m") or 0
    if ti>0.04: l+=1
    if ti<-0.04: s+=1
    rs=f.get("pair_vs_btc_return_z")
    if rs is not None and rs>0.25: l+=1
    if rs is not None and rs<-0.25: s+=1
    if rg=="TREND_UP": l+=2
    if rg=="TREND_DOWN": s+=2
    return ("LONG" if l>=s+2 else "SHORT" if s>=l+2 else "NEUTRAL"), {"long":l,"short":s}

def near_value(f, d):
    atr=f.get("atr_15m") or 0; c=f["last"]
    if atr<=0: return False
    levels=[x for x in [f.get("vwap_15m"),f.get("ema20_15m"),f.get("ema50_15m")] if x]
    if levels and min(abs(c-x) for x in levels)<=atr*envf("STAT_TECH_VALUE_ATR_BAND",0.85): return True
    z=f.get("vwap_dev_z_15m")
    return bool(z is not None and ((d=="LONG" and -1.8<=z<=0.35) or (d=="SHORT" and -0.35<=z<=1.8)))

def setup(f, rg, d):
    atr=f.get("atr_15m") or 0; c=f["last"]; hi=f.get("donchian_high_15m"); lo=f.get("donchian_low_15m"); volz=f.get("volume_z_15m") or 0; ti=f.get("taker_imbalance_15m") or 0
    trigger = (d=="LONG" and (f["last5"]>=f["ema20_5m"] or (f.get("return_5m_15m") or 0)>0.0015)) or (d=="SHORT" and (f["last5"]<=f["ema20_5m"] or (f.get("return_5m_15m") or 0)<-0.0015))
    if d in ("LONG","SHORT") and rg in ("TREND_UP","TREND_DOWN","NEUTRAL") and near_value(f,d) and trigger: return "TREND_PULLBACK", "trend_pullback_value_trigger", {}
    comp=(f.get("bb_width_pctile_15m") or 100)<envf("STAT_TECH_COMPRESSION_BB_PCTILE",25) and (f.get("atr_pctile_15m") or 100)<envf("STAT_TECH_COMPRESSION_ATR_PCTILE",35) and (f.get("range_width_pctile_15m") or 100)<envf("STAT_TECH_COMPRESSION_RANGE_PCTILE",30)
    if comp:
        if hi and c>hi and volz>=envf("STAT_TECH_BREAKOUT_VOLUME_Z_MIN",1.0) and ti>=-0.02: return "COMPRESSION_BREAKOUT", "compression_breakout_long", {"forced_direction":"LONG","breakout_level":hi}
        if lo and c<lo and volz>=envf("STAT_TECH_BREAKOUT_VOLUME_Z_MIN",1.0) and ti<=0.02: return "COMPRESSION_BREAKOUT", "compression_breakout_short", {"forced_direction":"SHORT","breakout_level":lo}
        return "WATCH_COMPRESSION", "compression_waiting_breakout", {}
    z=f.get("vwap_dev_z_15m")
    if rg=="RANGE" and d=="LONG" and lo and z is not None and z<-1.8 and c<=lo+atr*1.2 and trigger: return "RANGE_REVERSION_LIGHT", "range_low_vwap_reclaim", {}
    if rg=="RANGE" and d=="SHORT" and hi and z is not None and z>1.8 and c>=hi-atr*1.2 and trigger: return "RANGE_REVERSION_LIGHT", "range_high_vwap_reject", {}
    return "NONE", "no_balanced_setup", {}

def tech_score(f, rg, d, st):
    p={"regime":0,"direction":0,"setup_quality":0,"entry_location":0,"flow_confirm":0,"relative_strength":0}
    p["regime"] = 5 if rg in ("TREND_UP","TREND_DOWN") else 4 if rg in ("COMPRESSION","RANGE") else 2 if rg=="NEUTRAL" else 0
    p["direction"] = 5 if d in ("LONG","SHORT") else 1 if st=="WATCH_COMPRESSION" else 0
    p["setup_quality"] = 5 if st in ("TREND_PULLBACK","COMPRESSION_BREAKOUT") else 4 if st=="RANGE_REVERSION_LIGHT" else 3 if st=="WATCH_COMPRESSION" else 0
    p["entry_location"] = 4 if d in ("LONG","SHORT") and near_value(f,d) else 3 if st in ("COMPRESSION_BREAKOUT","RANGE_REVERSION_LIGHT") else 0
    ti=f.get("taker_imbalance_15m"); vz=f.get("volume_z_15m") or 0
    p["flow_confirm"] = 3 if (d=="LONG" and vz>=0 and (ti is None or ti>=-0.02)) or (d=="SHORT" and vz>=0 and (ti is None or ti<=0.02)) else 1 if d=="NEUTRAL" and vz>=0 else 0
    rs=f.get("pair_vs_btc_return_z"); rank=f.get("relative_strength_rank")
    if d=="LONG": p["relative_strength"] = 3 if ((rs is not None and rs>0.25) or (rank and rank<=5)) else 2 if rs is None or rs>-0.5 else 0
    if d=="SHORT": p["relative_strength"] = 3 if (rs is not None and rs<-0.25) else 2 if rs is None or rs<0.5 else 0
    return sum(p.values()), p

def plan(f,d,st,meta):
    e=float(f["last5"] or f["last"]); atr=f.get("atr_15m") or 0
    if atr<=0 or d not in ("LONG","SHORT"): return {"ok":False,"reason":"missing_atr_or_direction"}
    if st=="COMPRESSION_BREAKOUT":
        level=float(meta.get("breakout_level") or e); m=envf("STAT_TECH_BREAKOUT_SL_ATR_MULT",0.9); sl=level-atr*m if d=="LONG" else level+atr*m
    elif st=="RANGE_REVERSION_LIGHT":
        m=envf("STAT_TECH_RANGE_SL_ATR_MULT",0.75); sl=(float(f["donchian_low_15m"])-atr*m) if d=="LONG" else (float(f["donchian_high_15m"])+atr*m)
    else:
        m=envf("STAT_TECH_PULLBACK_SL_ATR_MULT",1.2); sl=min(float(f["swing_low_15m"]),e-atr*m) if d=="LONG" else max(float(f["swing_high_15m"]),e+atr*m)
    risk=abs(e-sl); rr=envf("RR_TARGET_R",1.2); tp=e+risk*rr if d=="LONG" else e-risk*rr
    if risk<=0 or risk/e>envf("STAT_TECH_MAX_SL_DISTANCE_PCT",0.04): return {"ok":False,"reason":"risk_invalid_or_too_wide","entry":e,"sl":sl,"risk_pct":risk/e if e else None}
    if d=="LONG" and not (sl<e<tp): return {"ok":False,"reason":"invalid_long_plan","entry":e,"sl":sl,"tp1":tp}
    if d=="SHORT" and not (sl>e>tp): return {"ok":False,"reason":"invalid_short_plan","entry":e,"sl":sl,"tp1":tp}
    return {"ok":True,"entry":e,"sl":sl,"tp1":tp,"tp2":tp,"tp3":tp,"risk":risk,"rr":rr,"target_mode":"SINGLE_FULL"}

def evaluate_symbol(symbol, rel=None):
    symbol=symbol.upper(); f, gap=features(symbol, rel or {})
    if not f: return {"created_at_utc":now_iso(),"marker":MARKER,"signal_source":"STAT_TECH_V1","symbol":symbol,"status":"INVALID","reason":"data_gap","counts":gap}
    rg=regime(f); d,ds=direction(f,rg); st,reason,meta=setup(f,rg,d)
    if meta.get("forced_direction"): d=meta["forced_direction"]
    sc,parts=tech_score(f,rg,d,st); status="IDLE"; grade="NONE"
    if st=="WATCH_COMPRESSION" or 12<=sc<=16: status="WATCH"; grade="WATCH"
    if sc>=envi("STAT_TECH_MIN_CANDIDATE_SCORE",17) and d in ("LONG","SHORT") and st not in ("NONE","WATCH_COMPRESSION"): status="CANDIDATE"; grade="A" if sc>=envi("STAT_TECH_A_GRADE_SCORE",21) else "B"
    pl={"ok":False,"reason":"not_candidate"}
    if status=="CANDIDATE":
        if rg=="HIGH_VOL": status="BLOCKED"; reason="blocked_high_vol"
        else:
            pl=plan(f,d,st,meta)
            if not pl.get("ok"): status="INVALID"; reason=pl.get("reason")
    clean={k:(round(v,8) if isinstance(v,float) else v) for k,v in f.items()}
    return {"created_at_utc":now_iso(),"marker":MARKER,"signal_source":"STAT_TECH_V1","symbol":symbol,"status":status,"setup_type":st,"direction":d,"regime":rg,"technical_score":sc,"technical_component":sc if status=="CANDIDATE" else 0,"technical_grade":grade,"score_parts":parts,"direction_scores":ds,"reason":reason,"features":clean,"entry":pl.get("entry"),"sl":pl.get("sl"),"tp1":pl.get("tp1"),"tp2":pl.get("tp2"),"tp3":pl.get("tp3"),"risk":pl.get("risk"),"rr":pl.get("rr"),"plan":pl}

def run_once(symbols=None, write_log=True):
    syms=[s.upper() for s in (symbols or symbols_default()) if str(s).strip()]
    syms=syms[:envi("STAT_TECH_MAX_SYMBOLS_PER_RUN",len(syms) or 14)]
    rel=rel_context(syms); rows=[evaluate_symbol(s,rel) for s in syms]
    if write_log:
        for r in rows: append_jsonl(log_dir()/"stat_tech_shadow_signals_v1.jsonl", r)
    def count(k):
        d={}
        for r in rows: d[r.get(k,"?")]=d.get(r.get(k,"?"),0)+1
        return d
    res={"ok":True,"created_at_utc":now_iso(),"marker":MARKER,"mode":os.getenv("STAT_TECH_MODE","SHADOW"),"total":len(rows),"by_status":count("status"),"by_setup":count("setup_type"),"by_regime":count("regime"),"candidates":[r for r in rows if r.get("status")=="CANDIDATE"],"watch":[r for r in rows if r.get("status")=="WATCH"]}
    if write_log: append_jsonl(log_dir()/"stat_tech_run_summary_v1.jsonl", res)
    return res

if __name__ == "__main__":
    import sys
    print(json.dumps(run_once([x for x in sys.argv[1:]] or None, True), ensure_ascii=False, indent=2))
