import { Component, type ErrorInfo, type ReactNode } from 'react';
import { AlertOctagon, RefreshCw } from 'lucide-react';

interface Props {
  children?: ReactNode;
  fallbackTitle?: string;
}

interface State {
  hasError: boolean;
  error: Error | null;
  errorInfo: ErrorInfo | null;
}

/**
 * React 异常边界组件 (ErrorBoundary)
 * 捕获子组件渲染过程中的 JS 崩溃异常，防止页面白屏 (White Screen of Death)，提供友好调试与重试界面。
 */
export class ErrorBoundary extends Component<Props, State> {
  public state: State = {
    hasError: false,
    error: null,
    errorInfo: null,
  };

  public static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error, errorInfo: null };
  }

  public componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error('ErrorBoundary caught an uncaught exception:', error, errorInfo);
    this.setState({ error, errorInfo });
  }

  private handleReset = () => {
    this.setState({ hasError: false, error: null, errorInfo: null });
  };

  public render() {
    if (this.state.hasError) {
      return (
        <div className="p-6 bg-slate-950/90 border border-red-900/60 rounded-2xl m-4 text-slate-100 shadow-2xl flex flex-col space-y-4">
          <div className="flex items-center space-x-3 text-red-400">
            <div className="p-2 bg-red-950/80 rounded-xl border border-red-800/80">
              <AlertOctagon className="w-6 h-6" />
            </div>
            <div>
              <h3 className="text-sm font-bold">
                {this.props.fallbackTitle || '组件渲染异常拦截 (ErrorBoundary)'}
              </h3>
              <p className="text-xs text-slate-400 mt-0.5">
                已自动拦截崩溃，防止整个应用白屏。以下为详细错误信息：
              </p>
            </div>
          </div>

          <div className="bg-slate-900 border border-slate-800 rounded-xl p-3.5 font-mono text-xs overflow-x-auto max-h-48 text-red-300">
            <div className="font-bold text-red-400 mb-1">
              {this.state.error?.name}: {this.state.error?.message}
            </div>
            {this.state.errorInfo?.componentStack && (
              <pre className="text-[10px] text-slate-400 whitespace-pre-wrap">
                {this.state.errorInfo.componentStack}
              </pre>
            )}
          </div>

          <div className="flex justify-end space-x-3">
            <button
              onClick={() => window.location.reload()}
              className="px-3.5 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-xl text-xs font-medium transition-colors cursor-pointer"
            >
              刷新整页
            </button>
            <button
              onClick={this.handleReset}
              className="flex items-center space-x-1.5 px-4 py-1.5 bg-gradient-to-r from-red-600 to-orange-600 hover:from-red-500 hover:to-orange-500 text-white rounded-xl text-xs font-medium shadow-md transition-all cursor-pointer"
            >
              <RefreshCw className="w-3.5 h-3.5" />
              <span>重试恢复组件</span>
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
