import { useEffect, useState } from 'react';
import { fetchTemplates, fetchTemplate, TemplateSummary, TemplateFull } from '../api/client';

interface Props {
  onSelect: (template: TemplateFull) => void;
}

/** 模板选择器——加载模板按钮，提供汇总表、标签表、多步骤加工三类模板 */
export function TemplateSelector({ onSelect }: Props) {
  const [templates, setTemplates] = useState<TemplateSummary[]>([]);
  const [loadErr, setLoadErr] = useState<string | null>(null);

  useEffect(() => {
    fetchTemplates()
      .then((res) => setTemplates(res.templates))
      .catch(() => setLoadErr('模板加载失败——API 不可用'));
  }, []);

  const handleClick = async (tpl: TemplateSummary) => {
    try {
      const full = await fetchTemplate(tpl.template_id);
      onSelect(full);
    } catch {
      setLoadErr(`模板 "${tpl.name}" 加载失败`);
    }
  };

  const categoryLabel: Record<string, string> = {
    aggregation: '汇总表',
    label: '标签表',
    multi_step: '多步骤',
    join: '关联宽表',
    window: '窗口排名',
    empty: '空白模板',
  };

  return (
    <div className="template-selector panel">
      <h3>📋 加载模板</h3>
      {loadErr && <div className="error-display" style={{ marginBottom: 8, fontSize: 11 }}>
        <span>{loadErr}</span>
      </div>}
      {templates.map((tpl) => (
        <button
          key={tpl.template_id}
          className="template-card"
          onClick={() => handleClick(tpl)}
        >
          <div className="tpl-name">{tpl.name}</div>
          <div className="tpl-desc">{tpl.description}</div>
          <span className="tpl-category">
            {categoryLabel[tpl.category] || tpl.category}
          </span>
        </button>
      ))}
      <div className="dry-run-notice" style={{ marginTop: 10 }}>
        ⚠️ 加载模板将替换当前编辑器内容
      </div>
    </div>
  );
}
