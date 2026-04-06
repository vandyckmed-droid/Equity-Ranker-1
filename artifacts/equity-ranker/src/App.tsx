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

const queryClient = new QueryClient();

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
  );
}

export default App;
