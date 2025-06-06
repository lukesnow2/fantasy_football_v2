"use client";

import { motion } from "framer-motion";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { 
  Crown, 
  TrendingUp, 
  Target, 
  Users, 
  Calendar,
  Award,
  BarChart3,
  Zap
} from "lucide-react";
import { useEffect, useState } from "react";
import Link from "next/link";

interface OverviewData {
  stats: {
    totalSeasons: number;
    totalMembers: number;
    totalGames: number;
    hallOfFame: number;
  };
  highlights: {
    currentChampion: {
      name: string;
      season: string;
    } | null;
    highestScorer: {
      name: string;
      score: number;
      season: string;
      week: string;
    } | null;
    bestDraftPick: {
      playerName: string;
      pickNumber: number;
      manager: string;
      season: string;
    } | null;
    mostChampionships: {
      manager: string;
      count: number;
    } | null;
  };
}

export function LeagueOverview() {
  const [data, setData] = useState<OverviewData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Add timestamp to prevent caching
    const timestamp = new Date().getTime();
    fetch(`/api/overview?t=${timestamp}`)
      .then(res => res.json())
      .then(response => {
        if (response.success) {
          setData(response.data);
        }
        setLoading(false);
      })
      .catch(err => {
        console.error('Failed to fetch overview data:', err);
        setLoading(false);
      });
  }, []);

  const highlights = [
    {
      icon: Crown,
      title: "Current Champion",
      description: data?.highlights.currentChampion ? `${data.highlights.currentChampion.season} Season Winner` : "Recent Winner",
      value: data?.highlights.currentChampion?.name || (loading ? "Loading..." : "No data"),
      badge: data?.highlights.currentChampion ? "Champion" : "TBD",
      progress: data?.highlights.currentChampion ? 100 : 0,
      color: "from-yellow-400 to-orange-500"
    },
    {
      icon: TrendingUp,
      title: "Highest Scorer",
      description: "Most points in a season",
      value: data?.highlights.highestScorer ? `${data.highlights.highestScorer.score.toLocaleString()} pts` : (loading ? "Loading..." : "No data"),
      badge: data?.highlights.highestScorer ? `${data.highlights.highestScorer.season} Record` : "Record",
      progress: data?.highlights.highestScorer ? 95 : 0,
      color: "from-green-400 to-blue-500"
    },
    {
      icon: Target,
      title: "Best Draft Pick",
      description: "Greatest value selection",
      value: data?.highlights.bestDraftPick?.playerName || (loading ? "Loading..." : "No data"),
      badge: data?.highlights.bestDraftPick ? `Pick ${data.highlights.bestDraftPick.pickNumber}, ${data.highlights.bestDraftPick.season}` : "Draft Pick",
      progress: data?.highlights.bestDraftPick ? 88 : 0,
      color: "from-purple-400 to-pink-500"
    },
    {
      icon: Award,
      title: "Most Championships",
      description: "Hall of Fame Leader",
      value: data?.highlights.mostChampionships ? `${data.highlights.mostChampionships.count} Titles` : (loading ? "Loading..." : "No data"),
      badge: data?.highlights.mostChampionships?.manager || "Champion",
      progress: data?.highlights.mostChampionships ? 92 : 0,
      color: "from-blue-400 to-indigo-500"
    }
  ];

  const quickStats = [
    { label: "Active Seasons", value: data?.stats.totalSeasons ? `${data.stats.totalSeasons} seasons` : (loading ? "..." : "0"), icon: Calendar },
    { label: "League Members", value: data?.stats.totalMembers ? data.stats.totalMembers.toString() : (loading ? "..." : "0"), icon: Users },
    { label: "Total Games", value: data?.stats.totalGames ? data.stats.totalGames.toLocaleString() : (loading ? "..." : "0"), icon: Zap },
    { label: "Hall of Fame", value: data?.stats.hallOfFame ? data.stats.hallOfFame.toString() : (loading ? "..." : "0"), icon: BarChart3 }
  ];

  return (
    <section className="py-20 bg-background">
      <div className="container">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          viewport={{ once: true }}
          className="text-center mb-16"
        >
          <h2 className="text-3xl md:text-5xl font-bold mb-4">
            League Highlights
          </h2>
          <p className="text-xl text-muted-foreground max-w-2xl mx-auto">
            From legendary drafts to championship runs, explore the moments that define our league
          </p>
        </motion.div>

        {/* Main Highlights Grid */}
        <motion.div 
          className="grid md:grid-cols-2 gap-8 mb-16"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          transition={{ duration: 0.8, staggerChildren: 0.1 }}
          viewport={{ once: true }}
        >
          {highlights.map((highlight, index) => (
            <motion.div
              key={highlight.title}
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: index * 0.1 }}
              viewport={{ once: true }}
            >
              <Card className="relative overflow-hidden hover:shadow-xl transition-all duration-300 group">
                <div className={`absolute inset-0 bg-gradient-to-br ${highlight.color} opacity-5 group-hover:opacity-10 transition-opacity`} />
                <CardHeader className="relative">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center space-x-3">
                      <div className={`p-3 rounded-lg bg-gradient-to-br ${highlight.color}`}>
                        <highlight.icon className="w-6 h-6 text-white" />
                      </div>
                      <div>
                        <CardTitle className="text-lg">{highlight.title}</CardTitle>
                        <CardDescription>{highlight.description}</CardDescription>
                      </div>
                    </div>
                    <Badge variant="outline" className="bg-background/50">
                      {highlight.badge}
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent className="relative">
                  <div className="space-y-4">
                    <div className="text-2xl font-bold">{highlight.value}</div>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Performance</span>
                        <span>{highlight.progress}%</span>
                      </div>
                      <Progress value={highlight.progress} className="h-2" />
                    </div>
                  </div>
                </CardContent>
              </Card>
            </motion.div>
          ))}
        </motion.div>

        {/* Quick Stats */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          viewport={{ once: true }}
        >
          <Card className="bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-gray-800 dark:to-gray-900 border-2">
            <CardHeader className="text-center">
              <CardTitle className="text-2xl">By the Numbers</CardTitle>
              <CardDescription>
                Two decades of fantasy football excellence in data
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
                {quickStats.map((stat, index) => (
                  <motion.div
                    key={stat.label}
                    className="text-center space-y-2"
                    initial={{ opacity: 0, scale: 0.9 }}
                    whileInView={{ opacity: 1, scale: 1 }}
                    transition={{ duration: 0.4, delay: index * 0.1 }}
                    viewport={{ once: true }}
                  >
                    <div className="mx-auto w-12 h-12 bg-primary/10 rounded-full flex items-center justify-center">
                      <stat.icon className="w-6 h-6 text-primary" />
                    </div>
                    <div className="text-2xl font-bold">{stat.value}</div>
                    <div className="text-sm text-muted-foreground">{stat.label}</div>
                  </motion.div>
                ))}
              </div>
            </CardContent>
          </Card>
        </motion.div>

        {/* Call to Action */}
        <motion.div
          className="text-center mt-16"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          viewport={{ once: true }}
        >
          <h3 className="text-2xl font-bold mb-4">Ready to Dive Deeper?</h3>
          <p className="text-muted-foreground mb-6 max-w-md mx-auto">
            Explore individual owner stats, season breakdowns, and advanced analytics
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link href="/dashboard">
              <Button size="lg">
                <Users className="w-4 h-4 mr-2" />
                View Dashboard
              </Button>
            </Link>
            <Link href="/analytics">
              <Button size="lg" variant="outline">
                <BarChart3 className="w-4 h-4 mr-2" />
                Analytics Dashboard
              </Button>
            </Link>
          </div>
        </motion.div>
      </div>
    </section>
  );
} 