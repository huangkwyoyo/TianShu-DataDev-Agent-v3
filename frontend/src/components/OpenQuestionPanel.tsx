import { OpenQuestionSummary } from '../api/client';

interface Props {
  questions: OpenQuestionSummary[];
}

/** OpenQuestion 面板——阻塞问题（红色）和非阻塞问题（黄色） */
export function OpenQuestionPanel({ questions }: Props) {
  if (questions.length === 0) return null;

  const blocking = questions.filter((q) => q.blocking);
  const nonBlocking = questions.filter((q) => !q.blocking);

  return (
    <div className="panel">
      <div className="panel-header">
        <h3>❓ OpenQuestion 面板</h3>
        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
          阻塞: {blocking.length} | 提示: {nonBlocking.length}
        </span>
      </div>

      {blocking.length > 0 && (
        <>
          <div className="section-title" style={{ color: 'var(--red)' }}>
            🚫 阻塞问题（必须解决才能继续）
          </div>
          {blocking.map((q) => (
            <div key={q.question_id} className="question-item question-blocking">
              <div className="question-source">[{q.source}] {q.question_id}</div>
              <div>{q.description}</div>
            </div>
          ))}
        </>
      )}

      {nonBlocking.length > 0 && (
        <>
          <div className="section-title" style={{ color: 'var(--yellow)' }}>
            ⚠️ 非阻塞问题（建议审查）
          </div>
          {nonBlocking.map((q) => (
            <div key={q.question_id} className="question-item question-nonblocking">
              <div className="question-source">[{q.source}] {q.question_id}</div>
              <div>{q.description}</div>
            </div>
          ))}
        </>
      )}

      {questions.length === 0 && (
        <div className="empty-state">无待解决问题</div>
      )}
    </div>
  );
}
