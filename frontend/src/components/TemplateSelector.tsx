import { useEffect, useState } from 'react';
import { fetchTemplates, fetchTemplate, TemplateSummary, TemplateFull } from '../api/client';

interface Props {
  onSelect: (template: TemplateFull) => void;
}

/** 模板选择器——头部下拉菜单，极简风格 */
export function TemplateSelector({ onSelect }: Props) {
  const [templates, setTemplates] = useState<TemplateSummary[]>([]);
  const [open, setOpen] = useState(false);
  const [loadErr, setLoadErr] = useState<string | null>(null);

  useEffect(() => {
    fetchTemplates()
      .then((res) => setTemplates(res.templates))
      .catch(() => setLoadErr('模板加载失败'));
  }, []);

  const handleClick = async (tpl: TemplateSummary) => {
    try {
      setOpen(false);
      const full = await fetchTemplate(tpl.template_id);
      onSelect(full);
    } catch {
      setLoadErr(`"${tpl.name}" 加载失败`);
    }
  };

  const categoryLabel: Record<string, string> = {
    aggregation: '汇总',
    label: '标签',
    multi_step: '多步',
    join: '关联',
    window: '窗口',
    empty: '空白',
  };

  return (
    <div className="header-template-select">
      <button
        className="header-template-btn"
        onClick={() => setOpen(!open)}
        title="加载模板"
      >
        Templates <span className="arrow">{open ? '▲' : '▼'}</span>
      </button>

      {open && (
        <>
          <div className="template-overlay" onClick={() => setOpen(false)} />
          <div className="template-dropdown">
            {loadErr && (
              <div className="template-dropdown-item" style={{ color: 'var(--error)', cursor: 'default' }}>
                {loadErr}
              </div>
            )}
            {templates.map((tpl) => (
              <button
                key={tpl.template_id}
                className="template-dropdown-item"
                onClick={() => handleClick(tpl)}
              >
                <span className="tpl-tag">{categoryLabel[tpl.category] || tpl.category}</span>
                <span className="tpl-name">{tpl.name}</span>
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
