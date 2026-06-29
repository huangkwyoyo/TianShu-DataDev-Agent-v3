import { PlanStepSummary } from '../api/client';

interface Props {
  steps: PlanStepSummary[];
  validationPassed: boolean;
  visible: boolean;
}

/** SqlBuildPlan 步骤面板——逐步骤展示执行计划 */
export function PlanStepsPanel({ steps, validationPassed, visible }: Props) {
  if (!visible) return null;

  return (
    <div className="panel">
      <div className="panel-header">
        <h3>📋 SqlBuildPlan 步骤</h3>
        <span
          className={`plan-validation ${
            validationPassed ? 'validation-passed' : 'validation-failed'
          }`}
        >
          {validationPassed ? '✓ 验证通过' : '✗ 验证未通过'}
        </span>
      </div>

      {steps.length === 0 ? (
        <div className="empty-state">无步骤数据</div>
      ) : (
        steps.map((step, i) => (
          <div key={step.step_id} className="step-item">
            <span className={`step-badge badge-${step.step_type}`}>
              {i + 1}. {step.step_type}
            </span>
            <span>{step.description}</span>
          </div>
        ))
      )}
    </div>
  );
}
