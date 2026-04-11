import React from "react";
import { Switch, Route, Router as WouterRouter } from "wouter";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Layout } from "@/components/layout/Layout";
import { PortfolioProvider } from "@/hooks/use-portfolio";
import MainPage from "@/pages/MainPage";
import PortfolioPage from "@/pages/PortfolioPage";
import MethodologyPage from "@/pages/MethodologyPage";
import NotFound from "@/pages/not-found";

class HMRErrorBoundary extends React.Component<{ children: React.ReactNode }, { crashed: boolean }> {
  private static lastReload = 0;
  constructor(props: { children: React.ReactNode }) {
    super(props);
    this.state = { crashed: false };
  }
  static getDerivedStateFromError() {
    return { crashed: true };
  }
  componentDidCatch(error: Error) {
    if (import.meta.env.DEV && error.message.includes("hooks")) {
      const now = Date.now();
      if (now - HMRErrorBoundary.lastReload > 3000) {
        HMRErrorBoundary.lastReload = now;
        setTimeout(() => window.location.reload(), 100);
      }
    }
  }
  render() {
    if (this.state.crashed) return <div style={{ background: "#0d1117", minHeight: "100vh" }} />;
    return this.props.children;
  }
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
      retry: 1,
    },
  },
});

function Router() {
  return (
    <Layout>
      <Switch>
        <Route path="/" component={MainPage} />
        <Route path="/portfolio" component={PortfolioPage} />
        <Route path="/methodology" component={MethodologyPage} />
        <Route component={NotFound} />
      </Switch>
    </Layout>
  );
}

function App() {
  return (
    <HMRErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <PortfolioProvider>
          <TooltipProvider>
            <WouterRouter base={import.meta.env.BASE_URL.replace(/\/$/, "")}>
              <Router />
            </WouterRouter>
            <Toaster />
          </TooltipProvider>
        </PortfolioProvider>
      </QueryClientProvider>
    </HMRErrorBoundary>
  );
}

export default App;
