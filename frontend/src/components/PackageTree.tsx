import { PackageRichResponse, ArtifactTreeNode } from '../api/client';

interface Props {
  pkg: PackageRichResponse;
  visible: boolean;
}

/** Review Package 文件树——展示 artifact 目录结构 */
export function PackageTree({ pkg, visible }: Props) {
  if (!visible) return null;

  return (
    <div className="panel">
      <div className="panel-header">
        <h3>📦 Review Package</h3>
        <span className="dry-run-notice">artifact 引用 · 不含文件内容</span>
      </div>

      {/* Package 元信息 */}
      <div className="pkg-meta">
        <span>Package: {pkg.package_id}</span>
        <span>创建: {pkg.created_at}</span>
        <span>Artifact 数: {pkg.artifact_count}</span>
        <span>返工轮次: {pkg.retry_count}</span>
      </div>

      {/* 文件树 */}
      {pkg.file_tree.length === 0 ? (
        <div className="empty-state">无 artifact 文件</div>
      ) : (
        <div className="file-tree">
          <TreeNode nodes={pkg.file_tree} />
        </div>
      )}
    </div>
  );
}

/** 递归渲染树节点 */
function TreeNode({ nodes, depth = 0 }: { nodes: ArtifactTreeNode[]; depth?: number }) {
  return (
    <>
      {nodes.map((node) => (
        <div
          key={node.path}
          className={`tree-node ${depth === 0 ? 'tree-node-root' : ''}`}
        >
          <div className="tree-node-name">
            <span className={node.kind === 'directory' ? 'tree-dir-icon' : 'tree-file-icon'}>
              {node.name}
            </span>
            {node.sha256 && (
              <span className="tree-sha">{node.sha256.substring(0, 12)}</span>
            )}
          </div>
          {node.children.length > 0 && (
            <TreeNode nodes={node.children} depth={depth + 1} />
          )}
        </div>
      ))}
    </>
  );
}
