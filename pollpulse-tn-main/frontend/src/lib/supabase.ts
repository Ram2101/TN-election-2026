import { createClient, SupabaseClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || ''
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || ''

// Only create Supabase client if credentials are provided
export const supabase: SupabaseClient | null = 
  supabaseUrl && supabaseAnonKey 
    ? createClient(supabaseUrl, supabaseAnonKey)
    : null

// Helper to check if Supabase is configured
export function isSupabaseConfigured(): boolean {
  return supabase !== null && !!supabaseUrl && !!supabaseAnonKey
}

// Types for our database
export interface ConstituencyPrediction {
  id: string
  constituency_name: string
  district: string
  alliance: string
  sentiment_score: number
  confidence_weight: number
  model_version: string
  source_count: number
  last_updated: string
}

export interface ConstituencyWinner {
  constituency_name: string
  district: string
  predicted_alliance: string
  sentiment_score: number
  confidence_weight: number
  weighted_score: number
  source_count: number
  baseline_margin?: number
  incumbent_2021?: string
  flip_probability?: number
  vulnerability_score?: number
  is_flip: boolean
  last_updated: string
}

export interface AllianceSummary {
  alliance: string
  avg_sentiment: number
  total_sources: number
  constituency_count: number
  districts_covered?: number
  prediction_count?: number
}

// Fetch functions
export async function fetchPredictions(): Promise<ConstituencyPrediction[]> {
  if (!supabase) {
    throw new Error('Supabase is not configured. Please set NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY in .env.local')
  }

  const { data, error } = await supabase
    .from('constituency_predictions')
    .select('*')
    .order('last_updated', { ascending: false })
  
  if (error) {
    console.error('Supabase error fetching predictions:', {
      message: error.message,
      details: error.details,
      hint: error.hint,
      code: error.code
    })
    
    // Provide more helpful error messages
    if (error.code === 'PGRST116') {
      throw new Error('Table "constituency_predictions" does not exist. Please run schema.sql in Supabase SQL Editor.')
    } else if (error.code === '42501') {
      throw new Error('Permission denied. Check Row Level Security (RLS) policies in Supabase.')
    } else if (error.message) {
      throw new Error(error.message)
    } else {
      throw error
    }
  }
  
  return data || []
}

// Fetch predicted winners (one per constituency)
export async function fetchConstituencyWinners(): Promise<ConstituencyWinner[]> {
  if (!supabase) {
    throw new Error('Supabase is not configured. Please set NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY in .env.local')
  }

  const { data, error } = await supabase
    .from('v_constituency_winners')
    .select('*')
    .order('constituency_name', { ascending: true })
  
  if (error) {
    console.error('Supabase error fetching winners:', {
      message: error.message,
      details: error.details,
      hint: error.hint,
      code: error.code
    })
    
    // Provide more helpful error messages
    if (error.code === 'PGRST116') {
      throw new Error('View "v_constituency_winners" does not exist. Please run schema.sql in Supabase SQL Editor.')
    } else if (error.code === '42501') {
      throw new Error('Permission denied. Check Row Level Security (RLS) policies in Supabase.')
    } else if (error.message) {
      throw new Error(error.message)
    } else {
      throw error
    }
  }
  
  return data || []
}

// Fetch alliance summary (from ALL predictions, not just winners)
export async function fetchAllianceSummary(): Promise<AllianceSummary[]> {
  if (!supabase) {
    throw new Error('Supabase is not configured. Please set NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY in .env.local')
  }

  const { data, error } = await supabase
    .from('v_alliance_summary')
    .select('*')
    .order('total_sources', { ascending: false })
  
  if (error) {
    console.error('Error fetching alliance summary:', error)
    throw error
  }
  
  return data || []
}

export async function fetchDistrictSummary(): Promise<AllianceSummary[]> {
  if (!supabase) {
    throw new Error('Supabase is not configured. Please set NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY in .env.local')
  }

  const { data, error } = await supabase
    .from('v_district_summary')
    .select('*')
  
  if (error) {
    console.error('Error fetching district summary:', error)
    throw error
  }
  
  return data || []
}

// Alliance color mapping (matches config/alliances_2026.json)
// Note: Backend uses DMK_Front/ADMK_Front, not DMK_Alliance/ADMK_Alliance
export const ALLIANCE_COLORS: Record<string, string> = {
  'DMK_Front': '#E31B23',
  'ADMK_Front': '#00A651',
  'TVK_Front': '#FFD700',
  'NTK': '#F39C12',
  'BJP_NDA': '#FF9933',
  'AMMK': '#9B59B6',
  'Neutral_Battleground': '#95A5A6',
  'Independent': '#95A5A6',
  'Others': '#7F8C8D',
  'Unknown': '#64748B',
}

export function getAllianceColor(alliance: string): string {
  return ALLIANCE_COLORS[alliance] || ALLIANCE_COLORS['Unknown']
}
