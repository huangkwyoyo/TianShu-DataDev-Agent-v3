import { useState } from 'react';
import { JoinEvidenceItem } from '../api/client';

interface Props {
  evidence: JoinEvidenceItem[];
}

/** Join 推理证据面板——展示 STRONG / MEDIUM / WEAK / NONE 四级证据 */
export function JoinEvidencePanel({ evidence }: Props) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  if (evidence.length === 0) {
    return (
      <div className="panel">
        <h3>🔗 Join 推理证据</h3>
        <div className="empty-state">无 Join 证据——可能为单表查询或无 Join 声明</div>
      </div>
    );
  }

  const toggleExpand = (id: string) => {
    setExpanded((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  const levelLabel: Record<string, string> = {
    STRONG: '强证据 · 自动采纳',
    MEDIUM: '中证据 · 需人工确认',
    WEAK: '弱证据 · 硬门禁阻断',
    NONE: '无证据 · 静默拒绝',
  };

  const stats = {
    STRONG: evidence.filter((e) => e.level === 'STRONG').length,
    MEDIUM: evidence.filter((e) => e.level === 'MEDIUM').length,
    WEAK: evidence.filter((e) => e.level === 'WEAK').length,
    NONE: evidence.filter((e) => e.level === 'NONE').length,
  };

  return (
    <div className="panel">
      <div className="panel-header">
        <h3>🔗 Join 推理证据</h3>
        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
          S:{stats.STRONG} M:{stats.MEDIUM} W:{stats.WEAK} N:{stats.NONE}
        </span>
      </div>

      {evidence.map((item) => (
        <div key={item.evidence_id} className="evidence-item">
          <div className="evidence-header">
            <span className="evidence-tables">
              {item.left_table} ⋈ {item.right_table}
            </span>
            <span className={`evidence-level level-${item.level}`}>
              {item.level} · {item.action}
            </span>
          </div>
          <div className="evidence-keys">
            左键: {item.left_key_raw} (归一化: {item.left_key_normalized})
            <br />
            右键: {item.right_key_raw} (归一化: {item.right_key_normalized})
          </div>
          <div className="evidence-detail">{levelLabel[item.level]}: {item.detail}</div>

          {/* 证据检查项列表 */}
          {item.evidence_checks.length > 0 && (
            <div className="evidence-checks">
              {item.evidence_checks.map((check, i) => (
                <div key={i} className="evidence-check-item">
                  {check.includes('MATCH') ? '✅' : check.includes('FAIL') ? '❌' : '•'} {check}
                </div>
              ))}
            </div>
          )}

          {/* 展开完整证据链 YAML */}
          {item.evidence_chain_yaml && (
            <>
              <button
                className="btn"
                style={{ marginTop: 4, fontSize: 10, padding: '2px 8px' }}
                onClick={() => toggleExpand(item.evidence_id)}
              >
                {expanded[item.evidence_id] ? '收起' : '展开'}证据链 YAML
              </button>
              {expanded[item.evidence_id] && (
                <pre className="evidence-chain">{item.evidence_chain_yaml}</pre>
              )}
            </>
          )}
        </div>
      ))}
    </div>
  );
}
