import React, { useEffect, useMemo, useState } from 'react';

function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length === 0) return [];
  const headers = lines[0].split(',').map(h => h.trim());
  return lines.slice(1).filter(l => l.trim().length > 0).map(line => {
    const cols = line.split(',');
    const obj = {};
    headers.forEach((h, i) => {
      obj[h] = (cols[i] || '').trim();
    });
    return obj;
  });
}

function numberFormat(n) {
  return new Intl.NumberFormat(undefined).format(n);
}

export default function AttributionSampleDashboard({ currentCsvUrl, yearlyCsvUrl, rankingCsvUrl }) {
  const [currentRows, setCurrentRows] = useState([]);
  const [yearlyRows, setYearlyRows] = useState([]);
  const [rankingRows, setRankingRows] = useState([]);
  const [scope, setScope] = useState('current'); // 'current' | 'yearly'
  const [selectedPeriod, setSelectedPeriod] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        const [curRes, yrRes, rankRes] = await Promise.all([
          fetch(currentCsvUrl),
          yearlyCsvUrl ? fetch(yearlyCsvUrl) : Promise.resolve({ ok: true, text: async () => '' }),
          rankingCsvUrl ? fetch(rankingCsvUrl) : Promise.resolve({ ok: true, text: async () => '' })
        ]);
        if (!curRes.ok) throw new Error(`Failed to load current CSV: ${curRes.status}`);
        if (yearlyCsvUrl && !yrRes.ok) throw new Error(`Failed to load yearly CSV: ${yrRes.status}`);
        if (rankingCsvUrl && !rankRes.ok) throw new Error(`Failed to load ranking CSV: ${rankRes.status}`);
        const curText = await curRes.text();
        const curData = parseCsv(curText);
        const yrText = yearlyCsvUrl ? await yrRes.text() : '';
        const yrData = yearlyCsvUrl ? parseCsv(yrText) : [];
        const rankText = rankingCsvUrl ? await rankRes.text() : '';
        const rankData = rankingCsvUrl ? parseCsv(rankText) : [];
        if (!cancelled) {
          setCurrentRows(curData);
          setYearlyRows(yrData);
          setRankingRows(rankData);
          // Default measurement period set later once options are computed
        }
      } catch (e) {
        if (!cancelled) setError(e.message || String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [currentCsvUrl, yearlyCsvUrl, rankingCsvUrl]);

  const measurementOptions = useMemo(() => {
    if (scope === 'current') {
      // Only show the latest date-like value (single option)
      const dates = Array.from(new Set(
        currentRows
          .map(r => r.as_of_date)
          .filter(v => v && String(v).includes('-'))
      )).sort();
      const latest = dates[dates.length - 1];
      return latest ? [latest] : [];
    } else {
      // Only show year-like values (exclude date-like tokens)
      const opts = Array.from(new Set(
        yearlyRows
          .map(r => r.performance_year)
          .filter(v => v && !String(v).includes('-'))
      )).sort();
      return opts;
    }
  }, [scope, currentRows, yearlyRows]);

  // Ensure selectedPeriod is always valid for current scope/options
  useEffect(() => {
    if (!measurementOptions.length) return;
    if (!selectedPeriod || !measurementOptions.includes(selectedPeriod)) {
      setSelectedPeriod(measurementOptions[measurementOptions.length - 1]);
    }
  }, [measurementOptions, selectedPeriod]);

  const filtered = useMemo(() => {
    if (scope === 'current') {
      return currentRows.filter(r => !selectedPeriod || (r.as_of_date || '') === selectedPeriod);
    } else {
      return yearlyRows.filter(r => !selectedPeriod || String(r.performance_year) === String(selectedPeriod));
    }
  }, [scope, currentRows, yearlyRows, selectedPeriod]);

  const kpis = useMemo(() => {
    const eligible = filtered.length;
    const attributed = filtered.filter(r => (r.provider_bucket || '').toLowerCase() !== 'no_eligible_history').length;
    const coverage = eligible > 0 ? (attributed / eligible) * 100 : 0;
    return { eligible, attributed, coverage };
  }, [filtered]);

  const STEP_DESCRIPTIONS = {
    1: '12-month PCP/NPP primary-care HCPCS',
    2: '12-month specialist primary-care HCPCS',
    3: '24-month PCP/NPP primary-care HCPCS',
    4: '24-month primary-care HCPCS (any classification)',
    5: '24-month any rendering NPI'
  };

  const byStep = useMemo(() => {
    const counts = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
    filtered.forEach(r => {
      const step = parseInt(r.assigned_step || '0', 10) || 0;
      if (step >= 1 && step <= 5) counts[step] = (counts[step] || 0) + 1;
    });
    return Object.keys(counts).map(k => ({ step: parseInt(k, 10), desc: STEP_DESCRIPTIONS[k], cnt: counts[k] }))
      .sort((a, b) => a.step - b.step);
  }, [filtered]);

  const topProviders = useMemo(() => {
    const counts = {};
    filtered.forEach(r => {
      const bucket = (r.provider_bucket || '').toLowerCase();
      if (bucket === 'no_eligible_history') return;
      const key = r.provider_id || 'unknown';
      if (!counts[key]) counts[key] = { provider_id: key, provider_bucket: r.provider_bucket || '', members: 0 };
      counts[key].members += 1;
    });
    return Object.values(counts).sort((a, b) => b.members - a.members).slice(0, 10);
  }, [filtered]);

  // Removed explicit fallback listing per spec

  if (loading) return <div>Loading sample dashboard…</div>;
  if (error) return <div style={{ color: 'red' }}>Error: {error}</div>;

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
        <label htmlFor="scope">Scope:</label>
        <select id="scope" value={scope} onChange={e => setScope(e.target.value)}>
          <option value="current">Current</option>
          <option value="yearly">Yearly</option>
        </select>

        <label htmlFor="period">{scope === 'current' ? 'As of date:' : 'Performance year:'}</label>
        <select id="period" value={selectedPeriod} onChange={e => setSelectedPeriod(e.target.value)}>
          {measurementOptions.map(opt => (
            <option key={String(opt)} value={String(opt)}>{String(opt)}</option>
          ))}
        </select>
      </div>

      <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', marginBottom: '1rem' }}>
        <div style={cardStyle}>
          <div style={cardLabel}>Eligible Members</div>
          <div style={cardValue}>{numberFormat(kpis.eligible)}</div>
        </div>
        <div style={cardStyle}>
          <div style={cardLabel}>Attributed Members</div>
          <div style={cardValue}>{numberFormat(kpis.attributed)}</div>
        </div>
        <div style={cardStyle}>
          <div style={cardLabel}>Coverage %</div>
          <div style={cardValue}>{kpis.coverage.toFixed(1)}%</div>
        </div>
      </div>

      <div style={{ display: 'flex', gap: '2rem', flexWrap: 'wrap' }}>
        <div style={{ flex: '1 1 480px' }}>
          <h4>Top Providers (by Panel Size)</h4>
          <table>
            <thead>
              <tr><th>Provider ID</th><th>Bucket</th><th>Members</th></tr>
            </thead>
            <tbody>
              {topProviders.map(p => (
                <tr key={p.provider_id}><td>{p.provider_id}</td><td>{p.provider_bucket}</td><td>{numberFormat(p.members)}</td></tr>
              ))}
            </tbody>
          </table>
        </div>

        <div style={{ flex: '1 1 320px' }}>
          <h4>Members by Assigned Step</h4>
          <table>
            <thead>
              <tr><th>Step</th><th>Description</th><th>Members</th></tr>
            </thead>
            <tbody>
              {byStep.map(row => (
                <tr key={row.step}><td>{row.step}</td><td>{row.desc}</td><td>{numberFormat(row.cnt)}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Removed sample fallback members table per spec */}

      {/* Footnote removed as requested */}
    </div>
  );
}

const cardStyle = {
  border: '1px solid #e0e0e0',
  borderRadius: 8,
  padding: '0.75rem 1rem',
  minWidth: 180,
  background: '#fafafa',
};

const cardLabel = { fontSize: 12, color: '#777', marginBottom: 6 };
const cardValue = { fontSize: 22, fontWeight: 600 };
