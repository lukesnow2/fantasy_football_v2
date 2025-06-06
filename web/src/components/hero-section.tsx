"use client";

import { motion } from "framer-motion";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Trophy, TrendingUp, Users, Calendar, Star } from "lucide-react";
import { useEffect, useState } from "react";
import Link from "next/link";

interface OverviewStats {
  totalSeasons: number;
  totalMembers: number;
  totalGames: number;
  hallOfFame: number;
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
    { icon: Trophy, label: "Championships", value: "...", description: "Total seasons" },
    { icon: Users, label: "League Members", value: "...", description: "Active owners" },
    { icon: TrendingUp, label: "Games Played", value: "...", description: "Since 2004" },
    { icon: Star, label: "Hall of Fame", value: "...", description: "Legendary owners" },
  ]);

  useEffect(() => {
    fetch('/api/overview')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.data?.stats) {
          const overviewStats: OverviewStats = data.data.stats;
          setStats([
            { icon: Trophy, label: "Championships", value: overviewStats.totalSeasons.toString(), description: "Total seasons" },
            { icon: Users, label: "League Members", value: overviewStats.totalMembers.toString(), description: "Active owners" },
            { icon: TrendingUp, label: "Games Played", value: overviewStats.totalGames.toLocaleString() + "+", description: "Since 2004" },
            { icon: Star, label: "Hall of Fame", value: overviewStats.hallOfFame.toString(), description: "Legendary owners" },
          ]);
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

          {/* Stats Cards */}
          <motion.div 
            className="grid grid-cols-2 md:grid-cols-4 gap-4 w-full max-w-4xl mt-16"
            initial="initial"
            animate="animate"
            variants={stagger}
          >
            {stats.map((stat) => (
              <motion.div key={stat.label} variants={fadeInUp}>
                <Card className="text-center hover:shadow-lg transition-shadow duration-300">
                  <CardHeader className="pb-2">
                    <div className="mx-auto w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center mb-2">
                      <stat.icon className="w-6 h-6 text-primary" />
                    </div>
                    <CardTitle className="text-2xl md:text-3xl font-bold">
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