"use client";

import { motion } from "framer-motion";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Trophy, TrendingUp, Users, Calendar, Star, Target, Activity, Zap } from "lucide-react";
import { useEffect, useState } from "react";
import Link from "next/link";

interface OverviewStats {
  totalSeasons: number;
  totalMembers: number;
  totalGames: number;
  hallOfFame: number;
  totalLeagues: number;
  activeLeagues: number;
}

interface LeagueInsights {
  mostCompetitive: string;
  competitivenessTier: string;
  balanceIndex: number;
  avgMarginOfVictory: number;
  transactionVolume: number;
}

interface APIResponse {
  data: {
    stats: OverviewStats;
    leagueInsights?: LeagueInsights;
  };
  meta?: {
    optimizedWithEDW: boolean;
    dataSource: string;
  };
}

const fadeInUp = {
  initial: { opacity: 0, y: 60 },
  animate: { opacity: 1, y: 0 },
  transition: { duration: 0.6 }
};

const stagger = {
  animate: {
    transition: {
      staggerChildren: 0.1
    }
  }
};

export function HeroSection() {
  const [stats, setStats] = useState([
    { icon: Trophy, label: "Championships", value: "...", description: "Total seasons", color: "from-yellow-400 to-orange-500" },
    { icon: Users, label: "League Members", value: "...", description: "Active owners", color: "from-blue-400 to-indigo-500" },
    { icon: Activity, label: "Games Played", value: "...", description: "Since 2004", color: "from-green-400 to-emerald-500" },
    { icon: Star, label: "Hall of Fame", value: "...", description: "Legendary owners", color: "from-purple-400 to-pink-500" },
  ]);

  const [leagueInsights, setLeagueInsights] = useState<LeagueInsights | null>(null);
  const [isEDWOptimized, setIsEDWOptimized] = useState(false);

  useEffect(() => {
    fetch('/api/overview')
      .then(res => res.json())
      .then((data: APIResponse) => {
        if (data.data?.stats) {
          const overviewStats = data.data.stats;
          setStats([
            { 
              icon: Trophy, 
              label: "Championships", 
              value: overviewStats.totalSeasons.toString(), 
              description: `Across ${overviewStats.totalLeagues} leagues`,
              color: "from-yellow-400 to-orange-500"
            },
            { 
              icon: Users, 
              label: "League Members", 
              value: overviewStats.totalMembers.toString(), 
              description: `Active in ${overviewStats.activeLeagues} leagues`,
              color: "from-blue-400 to-indigo-500"
            },
            { 
              icon: Activity, 
              label: "Games Played", 
              value: overviewStats.totalGames.toLocaleString() + "+", 
              description: "22 seasons of competition",
              color: "from-green-400 to-emerald-500"
            },
            { 
              icon: Star, 
              label: "Hall of Fame", 
              value: overviewStats.hallOfFame.toString(), 
              description: "Elite managers (3+ seasons)",
              color: "from-purple-400 to-pink-500"
            },
          ]);
          
          if (data.data.leagueInsights) {
            setLeagueInsights(data.data.leagueInsights);
          }
          
          if (data.meta?.optimizedWithEDW) {
            setIsEDWOptimized(true);
          }
        }
      })
      .catch(err => console.error('Failed to fetch stats:', err));
  }, []);

  return (
    <section className="relative overflow-hidden bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50 dark:from-gray-900 dark:via-gray-800 dark:to-gray-900">
      {/* Background Pattern */}
      <div className="absolute inset-0 bg-grid-pattern opacity-5" />
      
      <div className="container relative">
        <div className="flex flex-col items-center justify-center min-h-[80vh] text-center space-y-8">
          
          {/* Hero Content */}
          <motion.div 
            className="space-y-6 max-w-4xl"
            initial="initial"
            animate="animate"
            variants={stagger}
          >
            <motion.div variants={fadeInUp}>
              <Badge variant="secondary" className="text-sm px-4 py-2 mb-4">
                <Calendar className="w-4 h-4 mr-2" />
                2004 - 2025 • 22 Seasons of Glory
                {isEDWOptimized && (
                  <Zap className="w-3 h-3 ml-2 text-yellow-500" />
                )}
              </Badge>
            </motion.div>

            <motion.h1 
              className="text-5xl md:text-7xl font-bold bg-gradient-to-r from-blue-600 via-purple-600 to-indigo-600 bg-clip-text text-transparent"
              variants={fadeInUp}
            >
              The League
            </motion.h1>

            <motion.p 
              className="text-xl md:text-2xl text-gray-700 dark:text-gray-300 max-w-2xl mx-auto font-medium"
              variants={fadeInUp}
            >
              Two decades of fantasy football excellence. Dive deep into the stats, 
              relive the glory, and discover the legends.
            </motion.p>

            {/* League Insights Banner */}
            {leagueInsights && (
              <motion.div 
                className="bg-gradient-to-r from-blue-100 to-purple-100 dark:from-blue-900/30 dark:to-purple-900/30 rounded-lg p-4 max-w-2xl mx-auto"
                variants={fadeInUp}
              >
                <div className="flex items-center justify-center space-x-2 text-sm">
                  <Target className="w-4 h-4 text-blue-600" />
                  <span className="font-semibold">Most Competitive:</span>
                  <span>{leagueInsights.mostCompetitive}</span>
                  <Badge variant="outline" className="text-xs">
                    {leagueInsights.competitivenessTier}
                  </Badge>
                </div>
                <div className="text-xs text-muted-foreground mt-1">
                  Balance Index: {leagueInsights.balanceIndex} • Avg Margin: {leagueInsights.avgMarginOfVictory} pts
                </div>
              </motion.div>
            )}

            <motion.div 
              className="flex flex-col sm:flex-row gap-4 justify-center"
              variants={fadeInUp}
            >
              <Link href="/dashboard">
                <Button size="lg" className="text-lg px-8 py-6">
                  <Trophy className="w-5 h-5 mr-2" />
                  Explore Dashboard
                </Button>
              </Link>
              <Link href="/analytics">
                <Button size="lg" variant="outline" className="text-lg px-8 py-6">
                  <TrendingUp className="w-5 h-5 mr-2" />
                  View Analytics
                </Button>
              </Link>
            </motion.div>
          </motion.div>

          {/* Enhanced Stats Cards */}
          <motion.div 
            className="grid grid-cols-2 md:grid-cols-4 gap-4 w-full max-w-5xl mt-16"
            initial="initial"
            animate="animate"
            variants={stagger}
          >
            {stats.map((stat) => (
              <motion.div key={stat.label} variants={fadeInUp}>
                <Card className="text-center hover:shadow-lg transition-all duration-300 group hover:scale-105">
                  <CardHeader className="pb-2">
                    <div className={`mx-auto w-12 h-12 rounded-full bg-gradient-to-r ${stat.color} flex items-center justify-center mb-2 group-hover:scale-110 transition-transform`}>
                      <stat.icon className="w-6 h-6 text-white" />
                    </div>
                    <CardTitle className="text-2xl md:text-3xl font-bold bg-gradient-to-r from-gray-900 to-gray-600 dark:from-gray-100 dark:to-gray-300 bg-clip-text text-transparent">
                      {stat.value}
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="pt-0">
                    <CardDescription className="font-medium text-foreground">
                      {stat.label}
                    </CardDescription>
                    <p className="text-xs text-muted-foreground mt-1">
                      {stat.description}
                    </p>
                  </CardContent>
                </Card>
              </motion.div>
            ))}
          </motion.div>

          {/* Performance Badge */}
          {isEDWOptimized && (
            <motion.div
              initial={{ opacity: 0, scale: 0.8 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: 1.2, duration: 0.5 }}
              className="mt-8"
            >
              <Badge variant="outline" className="bg-gradient-to-r from-green-50 to-blue-50 dark:from-green-900/20 dark:to-blue-900/20 border-green-200 dark:border-green-800 text-green-700 dark:text-green-300">
                <Zap className="w-3 h-3 mr-1" />
                Powered by Enterprise Data Warehouse
              </Badge>
            </motion.div>
          )}

          {/* Floating Elements */}
          <motion.div
            className="absolute top-20 left-10 w-16 h-16 bg-gradient-to-r from-blue-400 to-purple-400 rounded-full opacity-20"
            animate={{
              y: [0, -20, 0],
              scale: [1, 1.1, 1],
            }}
            transition={{
              duration: 4,
              repeat: Infinity,
              ease: "easeInOut"
            }}
          />
          <motion.div
            className="absolute top-40 right-16 w-8 h-8 bg-gradient-to-r from-indigo-400 to-blue-400 rounded-full opacity-30"
            animate={{
              y: [0, 15, 0],
              x: [0, 10, 0],
            }}
            transition={{
              duration: 3,
              repeat: Infinity,
              ease: "easeInOut",
              delay: 1
            }}
          />
          <motion.div
            className="absolute bottom-20 left-20 w-12 h-12 bg-gradient-to-r from-purple-400 to-pink-400 rounded-full opacity-15"
            animate={{
              y: [0, -25, 0],
              rotate: [0, 180, 360],
            }}
            transition={{
              duration: 5,
              repeat: Infinity,
              ease: "easeInOut",
              delay: 2
            }}
          />
        </div>
      </div>
    </section>
  );
} 