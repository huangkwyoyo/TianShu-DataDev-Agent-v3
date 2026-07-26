import { useCallback, useEffect, useRef, useState } from 'react';
import { fetchTemplates, fetchTemplate, TemplateSummary, TemplateFull } from '../api/client';

interface Props {
  onSelect: (template: TemplateFull) => void;
}

/** 模板选择器——头部下拉菜单，极简风格 */
export function TemplateSelector({ onSelect }: Props) {
  const [templates, setTemplates] = useState<TemplateSummary[]>([]);
  const [open, setOpen] = useState(false);
  const [loadErr, setLoadErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const requestSequence = useRef(0);

  const loadTemplates = useCallback(async () => {
    const requestId = requestSequence.current + 1;
    requestSequence.current = requestId;
    setLoading(true);
    setLoadErr(null);
    try {
      const response = await fetchTemplates();
      if (!Array.isArray(response.templates)) {
        throw new Error('模板列表响应格式不正确');
      }
      if (requestId !== requestSequence.current) return;
      setTemplates(response.templates);
      setLoadErr(null);
    } catch {
      if (requestId !== requestSequence.current) return;
      setLoadErr('模板加载失败');
    } finally {
      if (requestId === requestSequence.current) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void loadTemplates();
  }, [loadTemplates]);

  const handleToggle = () => {
    const nextOpen = !open;
    setOpen(nextOpen);
    if (
      nextOpen
      && !loading
      && (loadErr !== null || templates.length === 0)
    ) {
      void loadTemplates();
    }
  };

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
        onClick={handleToggle}
        title="加载模板"
      >
        Templates <span className="arrow">{open ? '▲' : '▼'}</span>
      </button>

      {open && (
        <>
          <div className="template-overlay" onClick={() => setOpen(false)} />
          <div className="template-dropdown">
            {loading && (
              <div className="template-dropdown-item" style={{ cursor: 'default' }}>
                正在加载模板…
              </div>
            )}
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
